"""Coverage fixtures verify missing data is not reclassified as a halt or normal ST."""
from decimal import Decimal
import hashlib
import pandas as pd
import pytest

from stopat30m.data.ashare_history_audit import audit_daily_coverage, audit_qlib_availability, audit_reference_prices

A = "SH600000"


def sources():
    rows = pd.DataFrame([{"instrument": A, "date": day, "tradestatus": "1", "isST": "0",
        "source_complete_quote": True, "source_response_sha256": "synthetic-source"}
        for day in ("2023-01-03", "2023-01-04")])
    basic = pd.DataFrame([{"code": "sh.600000", "ipoDate": "1999-11-10", "outDate": ""}])
    return rows, basic, ["2022-12-30", "2023-01-03", "2023-01-04"]


def run(rows, basic, days):
    return audit_daily_coverage(rows, basic, days, [A], start="2023-01-01", end="2023-12-31")


def test_explicit_suspension_can_explain_a_missing_quote_but_not_an_absent_status():
    rows, basic, days = sources()
    rows.loc[1, ["tradestatus", "source_complete_quote"]] = ["0", False]
    result = run(rows, basic, days)
    assert result["status"] == "passed"
    assert result["source_confirmed_suspensions"][0]["session"] == "2023-01-04"
    rows.loc[1, "tradestatus"] = ""
    assert run(rows, basic, days)["status"] == "blocked"


def test_missing_listed_day_is_neither_normal_nor_a_verified_halt():
    rows, basic, days = sources()
    result = run(rows.iloc[:1], basic, days)
    assert result["status"] == "blocked"
    assert result["missing_listed_security_sessions"] == [{"instrument": A, "session": "2023-01-04"}]


def test_historical_st_and_trading_completeness_are_independent_requirements():
    rows, basic, days = sources()
    rows.loc[0, "isST"] = "1"
    assert run(rows, basic, days)["historical_st_row_count"] == 1
    rows.loc[0, "isST"] = None
    assert run(rows, basic, days)["status"] == "blocked"
    rows.loc[0, "isST"] = "0"
    rows.loc[0, "source_complete_quote"] = False
    assert run(rows, basic, days)["invalid_trading_sessions"][0]["reason"] == "incomplete_trading_quote"


def test_ipo_identity_may_explain_only_prelisting_absence():
    rows, basic, days = sources()
    basic.loc[0, "ipoDate"] = "2023-01-04"
    assert run(rows.iloc[1:], basic, days)["status"] == "passed"
    with pytest.raises(ValueError, match="Duplicate"):
        run(pd.concat([rows, rows.iloc[:1]]), basic, days)


def test_model_missing_bar_requires_actual_suspended_source_not_just_nan(monkeypatch):
    from stopat30m.data import ashare_history_audit as audit
    rows, _, _ = sources()
    stored = pd.DataFrame([[10., 10., 10., 10.], [float("nan")]*4],
        index=["2023-01-03", "2023-01-04"], columns=["open", "high", "low", "close"])
    monkeypatch.setattr(audit, "read_qlib_security", lambda *args: stored)
    result = audit_qlib_availability("synthetic", rows, [A], start="2023-01-01", end="2023-12-31")
    assert result["status"] == "blocked"
    rows.loc[1, ["tradestatus", "source_complete_quote"]] = ["0", False]
    result = audit_qlib_availability("synthetic", rows, [A], start="2023-01-01", end="2023-12-31")
    assert result["status"] == "passed"
    assert len(result["classified_missing_model_sessions"]) == 1


def rights_fixture():
    raw = pd.DataFrame([
        {"instrument": A, "date": "2023-06-14", "close": "2.74", "preclose": "2.74", "source_complete_quote": True},
        {"instrument": A, "date": "2023-06-27", "close": "2.54", "preclose": "2.57", "source_complete_quote": True},
    ])
    right = {"instrument": A, "ex_date": "2023-06-27", "event_id": "synthetic-rights",
        "actual_issued_ratio": "0.2889516483", "subscription_price": "2.02",
        "reference_preclose": "2.57", "independent_reference_verified": True}
    return raw, right, ["2023-06-14", "2023-06-27"]  # Explicit fixture sessions.


def test_rights_are_a_separate_actual_terms_audit_not_a_fabricated_free_bonus():
    raw, right, calendar = rights_fixture()
    result = audit_reference_prices(raw, [], calendar, start="2023-01-01", end="2023-12-31", rights=[right])
    assert result["status"] == "passed"
    detail = result["matched_adjustments"][0]
    assert detail["actual_issuance_theoretical_rounded"] == "2.58"
    assert detail["difference"] == "0.01" and detail["event_ids"] == []
    assert right["actual_issued_ratio"] == "0.2889516483"


def test_unverified_or_changed_rights_reference_never_closes_the_audit():
    raw, right, calendar = rights_fixture()
    for update in ({"independent_reference_verified": False}, {"reference_preclose": "2.58"}):
        with pytest.raises(ValueError, match="independent"):
            audit_reference_prices(raw, [], calendar, start="2023-01-01", end="2023-12-31", rights=[{**right, **update}])
    right["actual_issued_ratio"] = ".5"
    result = audit_reference_prices(raw, [], calendar, start="2023-01-01", end="2023-12-31", rights=[right])
    assert result["status"].startswith("blocked_")


def test_same_day_dividends_and_bonus_use_one_original_share_base():
    raw = pd.DataFrame([
        {"instrument": A, "date": "2023-01-03", "close": "10", "preclose": "10", "source_complete_quote": True},
        {"instrument": A, "date": "2023-01-04", "close": "4.90", "preclose": "4.85", "source_complete_quote": True}])
    events = [{"instrument": A, "ex_date": "2023-01-04", "event_id": "first", "cash_per_share": ".1", "bonus_ratio": "1"},
              {"instrument": A, "ex_date": "2023-01-04", "event_id": "special", "cash_per_share": ".2", "bonus_ratio": "0"}]
    result = audit_reference_prices(raw, events, ["2023-01-03", "2023-01-04"], start="2023-01-03", end="2023-01-04")
    assert result["status"] == "passed"
    assert result["matched_adjustments"][0]["expected_reference"] == "4.85"


def test_face_value_note_alone_cannot_hide_missing_stock_dividend_tax():
    from stopat30m.data.ashare_history_audit import audit_stock_dividend_tax
    event = {"event_id": "stock-dividend", "stock_dividend_ratio": ".5", "cash_per_share": "4.3",
             "taxable_stock_face_value_per_record_share": ".5", "tax_evidence_sha256": "actual-tax-proof",
             "cash_dividend_tax_rate": ".2"}
    assert audit_stock_dividend_tax([event]) == ["stock-dividend"]
    event["cash_dividend_tax_rate"] = ".224"  # Explicit conservative reserve.
    assert audit_stock_dividend_tax([event]) == []
    event["cash_per_share"] = "0"
    assert audit_stock_dividend_tax([event]) == ["stock-dividend"]


def test_partial_session_marks_do_not_turn_suspended_status_into_trade_permission(tmp_path):
    from stopat30m.data.ashare_history_audit import apply_intraday_observation_evidence
    proofs = []
    for name in ("issuer.pdf", "independent-daily.json"):
        path = tmp_path/name
        path.write_bytes(b"explicit synthetic issuer/quote evidence")
        proofs.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    raw = pd.DataFrame([{"instrument": A, "date": "2023-06-15", "tradestatus": "0", "isST": "0",
        "source_response_sha256": "original-row", "source_complete_quote": False,
        "open": "55.04", "high": "55.80", "low": "54.76", "close": "55.53", "preclose": "55.02",
        "volume": "353426", "amount": "19589785.99"}])
    event = {"instrument": A, "date": "2023-06-15", "source_response_sha256": "original-row",
        "execution_remains_blocked": True, "source_observed_open": "55.04", "source_observed_close": "55.53", "evidence": proofs}
    corrected = apply_intraday_observation_evidence(raw, {"events": [event]})
    assert corrected.iloc[0].source_observed_market_quote
    assert corrected.iloc[0].tradestatus == "0" and not corrected.iloc[0].source_complete_quote
    assert not raw.iloc[0].source_complete_quote and "source_observed_market_quote" not in raw
    (tmp_path/"issuer.pdf").write_bytes(b"changed proof")
    with pytest.raises(ValueError, match="evidence bytes"):
        apply_intraday_observation_evidence(raw, {"events": [event]})


def test_capital_reserve_exemption_is_never_inferred_from_its_name(tmp_path):
    from stopat30m.data.ashare_history_audit import audit_capital_reserve_tax
    event = {"instrument": A, "ex_date": "2023-06-15", "event_id": "capital", "capital_reserve_ratio": ".1"}
    assert audit_capital_reserve_tax([event], {"events": []}) == ["capital"]
    proof = tmp_path/"explicit-issuer-tax.txt"
    proof.write_text("synthetic issuer's explicit capital exemption confirmation")
    fact = {"instrument": A, "ex_date": "2023-06-15", "capital_reserve_tax_exempt": True,
        "evidence": [{"path": str(proof), "sha256": hashlib.sha256(proof.read_bytes()).hexdigest()}]}
    assert audit_capital_reserve_tax([event], {"events": [fact]}) == []
