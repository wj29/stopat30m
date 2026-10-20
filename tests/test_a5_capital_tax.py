from copy import deepcopy
from decimal import Decimal, localcontext
import hashlib
import json

import pytest

from stopat30m.data.a5_capital_tax import apply_a5_capital_tax
from stopat30m.data.ashare_actions import apply_action_overrides


def event(**updates):
    return dict(event_id="bs:688066", instrument="SH688066", record_date="2023-05-26",
        ex_date="2023-05-29", pay_date="2023-05-29", listing_date="2023-05-29",
        known_at="2023-05-23T23:59:59+08:00", cash_per_share="0.13",
        stock_dividend_ratio="0", capital_reserve_ratio="0.4", bonus_ratio="0.4",
        source_response_sha256="a" * 64, **updates)


def evidence(tmp_path, **updates):
    pdf = tmp_path / "issuer.pdf"
    pdf.write_bytes(b"%PDF-1.7\nindependent manually reviewed fixture")
    sha = hashlib.sha256(pdf.read_bytes()).hexdigest()
    fact = dict(instrument="SH688066", ex_date="2023-05-29", cash_per_share="0.13",
        stock_dividend_ratio="0", capital_reserve_ratio="0.4",
        status="verified_source_requires_tax_accounting", review_required=False,
        original_pdf_visual_reviewed=True, capital_reserve_tax_exempt=False,
        capital_reserve_taxable=True, source_account="other_capital_reserve",
        taxable_capital_face_value_per_record_share="0.4", share_face_value_cny="1",
        record_shares="185676281", transferred_shares="74270512",
        other_capital_reserve_debit_for_transfer_cny="74270512",
        per_record_share_entitlement_rounding_disclosed=True,
        evidence=[dict(path=str(pdf), sha256=sha, url="https://issuer.example/notice.pdf", pdf_pages_1based=[183])])
    fact.update(updates)
    return {"events": [fact]}


def test_evidenced_other_capital_reserved_from_cash_and_legacy_override_roundtrip(tmp_path):
    original = event()
    facts = evidence(tmp_path)
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "passed"
    assert report["covered_event_ids"] == [original["event_id"]]
    assert not report["tax_classification_unresolved"]
    with localcontext() as context:
        context.prec = 128
        assert Decimal(rows[0]["cash_dividend_tax_rate"]) * Decimal("0.13") >= Decimal("0.106")
        assert Decimal(rows[0]["cash_dividend_tax_rate"]) < Decimal("0.816")
    assert original == event()  # no in-place mutation of delivered terms
    assert rows[0]["cash_per_share"] == "0.13"
    assert rows[0]["bonus_ratio"] == "0.4"
    patch = tmp_path / "a5_annotations.json"
    patch.write_text(json.dumps(report["annotate_overrides"]))
    assert apply_action_overrides([original], patch) == rows
    assert set(report["annotate_overrides"]["patches"][0]["values"]) == {"cash_dividend_tax_rate"}


def test_unresolved_capital_source_stays_unresolved_under_explicit_cost_upper_bound(tmp_path):
    facts = evidence(tmp_path, capital_reserve_tax_classification="unresolved_conservative_upper_bound",
        status="verified_face_value_source_classification_unresolved", classification_verified=False,
        conservative_tax_reserve_upper_bound=True, capital_reserve_taxable=False,
        capital_reserve_face_value_per_record_share="0.4")
    rows, report = apply_a5_capital_tax([event()], facts)
    assert report["status"] == "passed"
    assert not report["all_source_classifications_verified"]
    assert report["tax_classification_unresolved"][0]["event_id"] == "bs:688066"
    assert report["applied"][0]["classification_verified"] is False
    assert rows[0]["cash_dividend_tax_rate"]


def test_exempt_capital_does_not_exempt_cash_or_overwrite_existing_stock_tax(tmp_path):
    facts = evidence(tmp_path, status="verified", capital_reserve_taxable=False, capital_reserve_tax_exempt=True)
    original = event()
    original["cash_dividend_tax_rate"] = "0.6"
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "passed"
    assert rows == [original]
    assert report["annotate_overrides"] == {"patches": []}


def schedule_evidence(tmp_path, **updates):
    values = dict(status="verified", capital_reserve_taxable=False, capital_reserve_tax_exempt=True,
        basis_kind="issuer_complete_personal_short_holding_tax_schedule_arithmetic",
        cash_tax_exemption_inferred=False,
        capital_exemption_inferred_from_issuer_total_tax_schedule=True,
        underlying_reserve_subaccount_independently_classified=False,
        issuer_short_holding_tax_per10="0.260000")
    values.update(updates)
    return evidence(tmp_path, **values)


def test_issuer_total_tax_schedule_inference_does_not_claim_premium_subaccount(tmp_path):
    rows, report = apply_a5_capital_tax([event()], schedule_evidence(tmp_path))
    assert report["status"] == "passed"
    assert rows == [event()]
    assert report["applied"][0]["classification"] == "exempt_issuer_implemented_tax_schedule"
    assert not report["all_reserve_subaccounts_identified"]
    assert report["capital_exemptions_inferred_from_issuer_tax_schedule"][0]["reserve_subaccount_identified"] is False
    assert not report["tax_classification_unresolved"]


def test_issuer_tax_display_rounding_does_not_hide_actual_entitlement_precision(tmp_path):
    original = event()
    original["cash_per_share"] = "0.0249999"
    facts = schedule_evidence(tmp_path, cash_per_share="0.0249999", issuer_short_holding_tax_per10="0.050000")
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "passed"
    assert rows[0]["cash_per_share"] == "0.0249999"


def test_older_individual_schedule_keeps_its_basis_without_claiming_premium(tmp_path):
    facts = evidence(tmp_path, status="verified", capital_reserve_taxable=False, capital_reserve_tax_exempt=True,
        basis_kind="arithmetic_from_explicit_issuer_personal_maximum_tax_schedule",
        basis="Issuer total personal tax equals the separately evidenced cash and stock dividend maximum tax.",
        inference_disclosed="Reserve subaccount not independently determined.")
    rows, report = apply_a5_capital_tax([event()], facts)
    assert report["status"] == "passed"
    assert rows == [event()]
    detail = report["applied"][0]
    assert detail["classification"] == "exempt_individually_evidenced"
    assert detail["basis_kind"] == facts["events"][0]["basis_kind"]
    assert detail["inference_disclosed"] == facts["events"][0]["inference_disclosed"]
    assert not detail["reserve_subaccount_identified"]
    assert not report["all_reserve_subaccounts_identified"]


def test_financial_reconciliation_inference_remains_separately_disclosed(tmp_path):
    facts = evidence(tmp_path, status="verified", capital_reserve_taxable=False, capital_reserve_tax_exempt=True,
        capital_premium_debit_for_transfer_cny="74270512", accounting_reconciliation_inference=True)
    _, report = apply_a5_capital_tax([event()], facts)
    assert report["status"] == "passed"
    assert report["applied"][0]["classification"] == "exempt_share_premium"
    assert report["capital_exemption_source_inferences"][0]["accounting_reconciliation_inference"] is True


@pytest.mark.parametrize("field,value", [
    ("issuer_short_holding_tax_per10", "1.060000"),
    ("issuer_short_holding_tax_per10", "0.260001"),
    ("issuer_short_holding_tax_per10", None),
    ("cash_tax_exemption_inferred", True),
    ("capital_exemption_inferred_from_issuer_total_tax_schedule", False),
    ("underlying_reserve_subaccount_independently_classified", True),
])
def test_tax_schedule_exemption_requires_complete_matching_individual_arithmetic(tmp_path, field, value):
    rows, report = apply_a5_capital_tax([event()], schedule_evidence(tmp_path, **{field: value}))
    assert report["status"] == "blocked"
    assert rows == [event()]


def test_tax_schedule_inference_cannot_exempt_an_unaccounted_stock_dividend(tmp_path):
    original = event()
    original["stock_dividend_ratio"] = "0.2"
    facts = schedule_evidence(tmp_path, stock_dividend_ratio="0.2")
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "blocked"
    assert rows == [original]


@pytest.mark.parametrize("cash", ["0", "0.01", "0.09"])
def test_cash_insufficiency_blocks_without_partial_annotation(tmp_path, cash):
    original = event()
    original["cash_per_share"] = cash
    facts = evidence(tmp_path, cash_per_share=cash)
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "blocked"
    assert "cannot cover" in report["pending"][0]["reason"]
    assert rows == [original]
    assert not report["covered_event_ids"]


@pytest.mark.parametrize("field,value", [
    ("cash_per_share", "0.14"), ("capital_reserve_ratio", "0.5"), ("stock_dividend_ratio", "0.01"),
    ("taxable_capital_face_value_per_record_share", None), ("share_face_value_cny", None),
    ("share_face_value_cny", "0.1"), ("other_capital_reserve_debit_for_transfer_cny", "74270511"),
    ("transferred_shares", "74270513"), ("per_record_share_entitlement_rounding_disclosed", False),
    ("capital_reserve_tax_exempt", True), ("review_required", True), ("cash_per_share", "NaN"),
])
def test_missing_face_mismatched_economics_or_source_reconciliation_blocks(tmp_path, field, value):
    original = event()
    rows, report = apply_a5_capital_tax([original], evidence(tmp_path, **{field: value}))
    assert report["status"] == "blocked"
    assert rows == [original]
    assert report["pending"][0]["event_id"] == original["event_id"]


def test_mixed_stock_and_capital_requires_evidenced_matching_stock_face_and_cash(tmp_path):
    original = event()
    original.update(cash_per_share="1", stock_dividend_ratio="0.2", bonus_ratio="0.6",
                    taxable_stock_face_value_per_record_share="0.2", tax_evidence_sha256="b" * 64)
    facts = evidence(tmp_path, cash_per_share="1", stock_dividend_ratio="0.2",
                     taxable_stock_face_value_per_record_share="0.2")
    original["tax_evidence_sha256"] = facts["events"][0]["evidence"][0]["sha256"]
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "passed"
    assert Decimal(rows[0]["cash_dividend_tax_rate"]) == Decimal("0.32")
    facts["events"][0]["taxable_stock_face_value_per_record_share"] = "0.1"
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "blocked"
    assert rows == [original]


def test_unbound_existing_stock_tax_proof_cannot_be_reused(tmp_path):
    original = event()
    original.update(cash_per_share="1", stock_dividend_ratio="0.2", bonus_ratio="0.6",
                    taxable_stock_face_value_per_record_share="0.2", tax_evidence_sha256="b" * 64)
    facts = evidence(tmp_path, cash_per_share="1", stock_dividend_ratio="0.2",
                     taxable_stock_face_value_per_record_share="0.2")
    rows, report = apply_a5_capital_tax([original], facts)
    assert report["status"] == "blocked"
    assert rows == [original]


def test_upper_bound_never_reduces_already_reserved_tax(tmp_path):
    original = event()
    original["cash_dividend_tax_rate"] = "0.95"
    rows, report = apply_a5_capital_tax([original], evidence(tmp_path))
    assert report["status"] == "passed"
    assert rows[0]["cash_dividend_tax_rate"] == "0.95"


def test_missing_fact_is_pending_and_changed_original_is_fatal(tmp_path):
    rows, report = apply_a5_capital_tax([event()], {"events": []})
    assert rows == [event()]
    assert report["status"] == "blocked"
    facts = evidence(tmp_path)
    (tmp_path / "issuer.pdf").write_bytes(b"different original")
    with pytest.raises(ValueError, match="bytes changed"):
        apply_a5_capital_tax([event()], facts)


def test_conflicting_duplicate_fact_or_delivered_event_is_rejected(tmp_path):
    facts = evidence(tmp_path)
    facts["events"].append(deepcopy(facts["events"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        apply_a5_capital_tax([event()], facts)
    with pytest.raises(ValueError, match="duplicate"):
        apply_a5_capital_tax([event(), event()], evidence(tmp_path))
