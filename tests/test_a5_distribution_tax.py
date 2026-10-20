from copy import deepcopy
from decimal import Decimal
import hashlib
import json

import pytest

from stopat30m.backtest.ashare_execution import PortfolioState, Position
from stopat30m.data.a5_capital_tax import apply_a5_capital_tax
from stopat30m.data import a5_distribution_tax as tax


def event(**updates):
    row = dict(event_id="bs:603505:2025", instrument="SH603505", record_date="2025-07-14",
        ex_date="2025-07-15", pay_date="2025-07-15", listing_date="2025-07-15",
        known_at="2025-07-09T23:59:59+08:00", source_response_sha256="a" * 64,
        cash_per_share=".05", stock_dividend_ratio=".4", capital_reserve_ratio="0", bonus_ratio=".4")
    row.update(updates)
    return row


def source(tmp_path):
    path = tmp_path / "issuer.pdf"
    if not path.exists():
        path.write_bytes(b"%PDF source fixture independently reviewed in this unit test")
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                url="https://issuer.example/notice.pdf", pdf_pages_1based=[1, 4])


def stock_fact(tmp_path, row=None, **updates):
    row = row or event()
    fact = {key: row[key] for key in tax.TERMS}
    fact.update(status="verified_stock_tax_base_pending_separate_liability_accounting",
        issuer_actual_terms_verified=True, original_pdf_visual_reviewed=True, review_required=False,
        evidence=[source(tmp_path)], share_face_value_cny="1",
        taxable_stock_face_value_per_record_share=row["stock_dividend_ratio"])
    fact.update(updates)
    return fact


def capital_fact(tmp_path, row, **updates):
    fact = {key: row[key] for key in tax.TERMS}
    fact.update(status="verified_face_value_source_classification_unresolved", review_required=False,
        original_pdf_visual_reviewed=True, evidence=[source(tmp_path)],
        capital_reserve_tax_classification="unresolved_conservative_upper_bound", classification_verified=False,
        conservative_tax_reserve_upper_bound=True, capital_reserve_tax_exempt=False, capital_reserve_taxable=False,
        share_face_value_cny="1", capital_reserve_face_value_per_record_share=row["capital_reserve_ratio"])
    fact.update(updates)
    return fact


def prepared(tmp_path, row=None):
    row = row or event()
    return tax.prepare_distribution_tax([row], {"events": []}, {"events": [stock_fact(tmp_path, row)]})


def state(cash="100", quantity=1000):
    return PortfolioState(cash=Decimal(cash), positions={"SH603505": Position(1)},
                          holdings_history={"2025-07-14": {"SH603505": quantity}})


def test_stock_tax_overflow_reserves_actual_record_holdings_and_keeps_delivered_shares(tmp_path):
    original = event()
    rows, report = prepared(tmp_path, original)
    assert report["status"] == "passed"
    assert rows[0]["cash_dividend_tax_rate"] == "1.00000000000000000000000000000000000000000000000000"
    assert rows[0]["stock_dividend_ratio"] == ".4"
    assert original == event()
    assert Decimal(report["contracts"][0]["full_tax_reserve_per_record_share"]) == Decimal(".09")
    before = state()
    after, journal = tax.reserve_additional_distribution_tax(before, "2025-07-15", report["contracts"])
    assert after.cash == Decimal("60") and before.cash == Decimal("100")
    assert after.positions == before.positions
    assert journal[0]["record_date_shares"] == 1000  # not today's one share
    assert Decimal(journal[0]["additional_tax_reserved_cny"]) == 40
    assert Decimal(journal[0]["legacy_cash_tax_reserved_cny"]) == 50


@pytest.mark.parametrize("ratio,expected", [(".4", ".08"), (".45", ".09")])
def test_pure_capital_zero_cash_has_complete_explicit_cost_without_fake_exemption(tmp_path, ratio, expected):
    row = event(cash_per_share="0", stock_dividend_ratio="0", capital_reserve_ratio=ratio, bonus_ratio=ratio)
    rows, report = tax.prepare_distribution_tax([row], {"events": [capital_fact(tmp_path, row)]}, {"events": []})
    assert report["status"] == "passed"
    assert not report["all_source_classifications_verified"]
    contract = report["contracts"][0]
    assert Decimal(contract["additional_tax_reserve_per_record_share"]) == Decimal(expected)
    after, journal = tax.reserve_additional_distribution_tax(state(), row["ex_date"], {"contracts": [contract]})
    assert Decimal(journal[0]["additional_tax_reserved_cny"]) == Decimal(expected) * 1000
    assert after.cash == Decimal(100) - Decimal(expected) * 1000
    assert rows[0]["capital_reserve_ratio"] == ratio


def test_ordinary_already_covered_capital_event_is_economically_unchanged(tmp_path):
    row = event(cash_per_share=".13", stock_dividend_ratio="0", capital_reserve_ratio=".4")
    fact = capital_fact(tmp_path, row)
    legacy_rows, legacy = apply_a5_capital_tax([row], {"events": [fact]})
    assert legacy["status"] == "passed"
    rows, report = tax.prepare_distribution_tax(legacy_rows, {"events": [fact]}, {"events": []})
    assert report["status"] == "passed"
    assert rows == legacy_rows
    assert not report["contracts"]
    before = state()
    after, journal = tax.reserve_additional_distribution_tax(before, row["ex_date"], [])
    assert after.to_dict() == before.to_dict() and not journal


def test_replay_never_charges_again_on_ex_pay_or_listing_day(tmp_path):
    _, report = prepared(tmp_path)
    first, _ = tax.reserve_additional_distribution_tax(state(), "2025-07-15", report["contracts"])
    again, journal = tax.reserve_additional_distribution_tax(first, "2025-07-15", report["contracts"])
    later, later_journal = tax.reserve_additional_distribution_tax(again, "2025-07-16", report["contracts"])
    assert first.to_dict() == again.to_dict() == later.to_dict()
    assert not journal and not later_journal
    restored = PortfolioState.from_dict(json.loads(json.dumps(first.to_dict())))
    replay, _ = tax.reserve_additional_distribution_tax(restored, "2025-07-16", report["contracts"])
    assert replay.to_dict() == first.to_dict()


def test_insufficient_cash_atomic_failure_and_no_borrowing(tmp_path):
    _, report = prepared(tmp_path)
    before = state(cash="39.99")
    snapshot = before.to_dict()
    with pytest.raises(tax.AdditionalTaxFundingError, match="Insufficient existing cash"):
        tax.reserve_additional_distribution_tax(before, "2025-07-15", report["contracts"])
    assert before.to_dict() == snapshot


def test_multiple_events_failed_second_reserve_does_not_partially_charge_input(tmp_path):
    row1 = event()
    row2 = event(event_id="second", instrument="SH603506")
    _, report = tax.prepare_distribution_tax([row1, row2], {"events": []},
        {"events": [stock_fact(tmp_path, row1), stock_fact(tmp_path, row2)]})
    before = state(cash="60")
    before.holdings_history["2025-07-14"]["SH603506"] = 1000
    snapshot = before.to_dict()
    with pytest.raises(tax.AdditionalTaxFundingError):
        tax.reserve_additional_distribution_tax(before, "2025-07-15", report["contracts"])
    assert before.to_dict() == snapshot


def test_rounding_topup_covers_full_tax_when_legacy_gross_rounds_down(tmp_path):
    row = event(cash_per_share=".054", stock_dividend_ratio="0", capital_reserve_ratio=".366")
    _, report = tax.prepare_distribution_tax([row], {"events": [capital_fact(tmp_path, row)]}, {"events": []})
    _, journal = tax.reserve_additional_distribution_tax(state(quantity=1), row["ex_date"], report["contracts"])
    assert Decimal(journal[0]["legacy_cash_tax_reserved_cny"]) == Decimal(".05")
    assert Decimal(journal[0]["additional_tax_reserved_cny"]) == Decimal(".04")
    assert Decimal(journal[0]["rounding_coverage_topup_cny"]) == Decimal(".01")


@pytest.mark.parametrize("field,value", [("record_date", "2025-07-11"), ("cash_per_share", ".051"),
    ("capital_reserve_ratio", ".1"), ("stock_dividend_ratio", ".41"), ("share_face_value_cny", None),
    ("taxable_stock_face_value_per_record_share", ".39")])
def test_actual_terms_and_nominal_cannot_be_inferred_or_mismatched(tmp_path, field, value):
    fact = stock_fact(tmp_path, **{field: value})
    rows, report = tax.prepare_distribution_tax([event()], {"events": []}, {"events": [fact]})
    assert report["status"] == "blocked" and report["pending"]
    assert rows == [event()] and not report["contracts"]


def test_missing_record_snapshot_never_uses_payment_day_position(tmp_path):
    _, report = prepared(tmp_path)
    before = state()
    before.holdings_history = {}
    with pytest.raises(ValueError, match="record-date"):
        tax.reserve_additional_distribution_tax(before, "2025-07-15", report["contracts"])


def test_late_known_or_not_yet_known_clock_is_rejected(tmp_path):
    row = event(known_at="2025-07-15T10:00:00+08:00")
    rows, report = prepared(tmp_path, row)
    assert report["status"] == "blocked" and rows == [row]
    _, report = prepared(tmp_path)
    with pytest.raises(ValueError, match="not known"):
        tax.reserve_additional_distribution_tax(state(), "2025-07-15", report["contracts"],
                                               now="2025-07-09T09:30:00+08:00")


def test_changed_contract_terms_or_record_snapshot_replay_is_rejected(tmp_path):
    _, report = prepared(tmp_path)
    after, _ = tax.reserve_additional_distribution_tax(state(), "2025-07-15", report["contracts"])
    changed = deepcopy(report["contracts"])
    changed[0]["additional_tax_reserve_per_record_share"] = ".01"
    with pytest.raises(ValueError, match="terms changed"):
        tax.reserve_additional_distribution_tax(after, "2025-07-15", changed)
    changed[0]["terms_sha256"] = tax._digest({k: v for k, v in changed[0].items() if k != "terms_sha256"})
    with pytest.raises(ValueError, match="arithmetic differs"):
        tax.reserve_additional_distribution_tax(after, "2025-07-15", changed)
    after.holdings_history["2025-07-14"]["SH603505"] = 999
    with pytest.raises(ValueError, match="changed terms or record holdings"):
        tax.reserve_additional_distribution_tax(after, "2025-07-15", report["contracts"])


def test_changed_original_and_nested_supporting_proof_are_rejected(tmp_path):
    extra = tmp_path / "reviewed-page.png"
    extra.write_bytes(b"original page")
    fact = stock_fact(tmp_path)
    fact["supporting_evidence"] = [{"path": str(extra), "sha256": hashlib.sha256(extra.read_bytes()).hexdigest()}]
    _, report = tax.prepare_distribution_tax([event()], {"events": []}, {"events": [fact]})
    extra.write_bytes(b"modified page")
    with pytest.raises(ValueError, match="evidence bytes changed"):
        tax.reserve_additional_distribution_tax(state(), "2025-07-15", report["contracts"])
    with pytest.raises(ValueError, match="evidence bytes changed"):
        tax.prepare_distribution_tax([event()], {"events": []}, {"events": [fact]})


def test_source_mutation_during_preparation_is_detected(tmp_path, monkeypatch):
    original = tax._resolved
    def changed(*args):
        value = original(*args)
        (tmp_path / "issuer.pdf").write_bytes(b"modified during validation")
        return value
    monkeypatch.setattr(tax, "_resolved", changed)
    with pytest.raises(ValueError, match="changed during validation"):
        prepared(tmp_path)


def test_no_duplicate_contract_and_no_reserve_after_day_already_finalized(tmp_path):
    _, report = prepared(tmp_path)
    with pytest.raises(ValueError, match="Duplicate"):
        tax.reserve_additional_distribution_tax(state(), "2025-07-15", report["contracts"] * 2)
    before = state()
    before.holdings_history["2025-07-15"] = {"SH603505": 1000}
    with pytest.raises(ValueError, match="before finalized"):
        tax.reserve_additional_distribution_tax(before, "2025-07-15", report["contracts"])


def test_contract_binds_only_its_own_sources_but_report_binds_entire_input(tmp_path):
    row = event()
    unrelated_row = event(event_id="unrelated", instrument="SH603506")
    other = tmp_path / "other"
    other.mkdir()
    facts = [stock_fact(tmp_path, row), stock_fact(other, unrelated_row)]
    _, report = tax.prepare_distribution_tax([row], {"events": []}, {"events": facts})
    all_paths = {item["path"] for item in report["bound_inputs"]}
    contract_paths = {item["path"] for item in report["contracts"][0]["bound_inputs"]}
    assert str(other / "issuer.pdf") in all_paths
    assert str(other / "issuer.pdf") not in contract_paths
    assert str(tmp_path / "issuer.pdf") in contract_paths
    # A different event's original remains bound at whole-run boundaries, while
    # this execution verifies only this contract's complete original evidence.
    (other / "issuer.pdf").write_bytes(b"unrelated source changed")
    after, _ = tax.reserve_additional_distribution_tax(state(), "2025-07-15", report["contracts"])
    assert after.cash == Decimal("60")
