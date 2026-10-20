from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import numpy as np
import pytest

from stopat30m.data import a5_stability_market_audit as audit
from stopat30m.data.ashare_actions import SOURCE


def source_row(**changes):
    return {"code": "sh.600000", "dividPlanDate": "2025-06-01",
        "dividRegistDate": "2025-06-10", "dividOperateDate": "2025-06-11",
        "dividPayDate": "2025-06-11", "dividStockMarketDate": "",
        "dividCashPsBeforeTax": "0.432", "dividStocksPs": "0.000000",
        "dividReserveToStockPs": "", "dividCashPsAfterTax": "0.389或0.432",
        "dividCashStock": "cash", **changes}


def archive(tmp_path, rows):
    root = tmp_path / "archive"; root.mkdir()
    fields = list(rows[0]); path = root / "SH600000_2025.json"
    payload = {"request": {"code": "sh.600000", "year": "2025", "yearType": "operate"},
        "fields": fields, "rows": [[r[f] for f in fields] for r in rows],
        "generation": "generation", "fetched_at": "2026-10-20T01:00:00+00:00"}
    audit._save(path, payload)
    manifest = {"source": SOURCE, "year_type": "operate", "status": "complete", "failures": {},
        "generation": "generation", "as_of_date": "2026-10-20", "instruments": ["SH600000"],
        "years": [2025], "responses": {"SH600000_2025": {"file": path.name,
            "sha256": audit.sha(path), "rows": len(rows), "fetched_at": payload["fetched_at"]}}}
    audit._save(root / "collection.json", manifest)
    return root, path


def fact(tmp_path, root, original, rows, *, canonical=0):
    pdf = tmp_path / "issuer.pdf"; pdf.write_bytes(b"synthetic issuer fixture")
    png = tmp_path / "issuer_page.png"; png.write_bytes(b"synthetic rendered page fixture")
    event = {"status": "verified_single_issuer_distribution_duplicate_provider_annotations",
        "original_pdf_visual_reviewed": True, "execution_entitlement_issuer_verified": True,
        "collapse_exact_rows_only": True, "economic_fields_are_equal_across_original_duplicates": True,
        "economic_events_after_resolution": 1, "instrument": "SH600000", "ex_date": "2025-06-11",
        "record_date": "2025-06-10", "pay_date": "2025-06-11", "cash_per_share": "0.432",
        "stock_dividend_ratio": "0", "capital_reserve_ratio": "0",
        "original_response": audit._proof(original), "original_duplicate_rows": [
            {"row_index_zero_based": n, "row": r, "canonical_row_json_sha256": audit._canonical_row_sha(r)}
            for n, r in enumerate(rows)], "canonical_source_row_index_zero_based": canonical,
        "canonical_source_row": rows[canonical], "ignored_conflicting_annotations": ["dividCashPsAfterTax", "dividCashStock"],
        "non_conflicting_optional_metadata_union_allowed": False,
        "evidence": [{**audit._proof(pdf), "url": "https://issuer.example/implementation.pdf"}, audit._proof(png)]}
    path = tmp_path / "resolution.json"
    audit._save(path, {"kind": "a5_issuer_verified_duplicate_distribution_resolution",
        "status": "source_resolution_verified_not_full_market_gate", "events": [event],
        "source_manifest": audit._proof(root / "collection.json"), "bound_inputs": []})
    return path


def normalize(root, **kwargs):
    return audit.normalize_stability_dividends(root, start="2025-01-01", end="2026-10-19", **kwargs)


def test_exact_issuer_row_resolution_keeps_payload_bytes_and_one_economic_event(tmp_path):
    rows = [source_row(), source_row(dividCashPsAfterTax="0.3888或0.432")]
    root, original = archive(tmp_path, rows); before = original.read_bytes()
    path = fact(tmp_path, root, original, rows)
    events, receipt = normalize(root, resolution_paths=[path])
    assert original.read_bytes() == before
    assert len(events) == 1 and events[0]["cash_per_share"] == "0.432"
    assert events[0]["source_response_sha256"] == audit.sha(original)
    assert receipt["exact_issuer_resolutions"][0]["original_row_indices_zero_based"] == [0, 1]
    assert receipt["source_bytes_modified"] is False
    assert any(p["path"].endswith("issuer_page.png") for p in receipt["bound_inputs"].values())


def test_unregistered_annotation_conflict_cannot_be_dropped(tmp_path):
    root, _ = archive(tmp_path, [source_row(), source_row(dividCashPsAfterTax="0.3888")])
    with pytest.raises(ValueError, match="Unresolved conflicting"):
        normalize(root)


@pytest.mark.parametrize("mutation", ["index", "economic", "third_row", "canonical", "visual"])
def test_resolution_cannot_expand_beyond_exact_rows_indices_economics_and_visuals(tmp_path, mutation):
    rows = [source_row(), source_row(dividCashPsAfterTax="0.3888")]
    if mutation == "economic":rows[1]["dividCashPsBeforeTax"] = "0.5"
    root, original = archive(tmp_path, rows + ([source_row()] if mutation == "third_row" else []))
    path = fact(tmp_path, root, original, rows)
    document = json.loads(path.read_text()); event = document["events"][0]
    if mutation == "index":event["original_duplicate_rows"][1]["row_index_zero_based"] = 0
    if mutation == "canonical":event["canonical_source_row"]["dividPayDate"] = "2025-06-12"
    if mutation == "visual":event["original_pdf_visual_reviewed"] = False
    audit._save(path, document)
    with pytest.raises(ValueError):normalize(root, resolution_paths=[path])


def test_legacy_date_view_requires_frozen_manifest_and_complete_original_row(tmp_path, monkeypatch):
    rows = [source_row(dividPlanDate="", dividCashPsAfterTax=""), source_row()]
    root, original = archive(tmp_path, rows); before = original.read_bytes(); proof = audit._proof(root / "collection.json")
    with pytest.raises(ValueError):normalize(root, legacy_complete_row_basis=proof)
    monkeypatch.setattr(audit, "LEGACY_ARCHIVE_MANIFEST_SHA256", frozenset({proof["sha256"]}))
    events, receipt = normalize(root, legacy_complete_row_basis=proof)
    assert len(events) == 1 and original.read_bytes() == before
    view = receipt["legacy_complete_disclosure_views"][0]
    assert view["incomplete_view_row_index_zero_based"] == 0 and view["complete_view_row_index_zero_based"] == 1
    assert view["complete_original_row"] == rows[1] and view["issuer_new_verification_claimed"] is False


def test_missing_payment_date_remains_unknown_and_requires_bound_issuer_override(tmp_path):
    root, original = archive(tmp_path, [source_row(dividPayDate="")])
    events, receipt = normalize(root)
    assert events[0]["pay_date"] is None and receipt["unresolved_source_rows"]
    audit._close_normalization_pending(events, [receipt], {})
    assert receipt["unresolved_source_rows"]
    events[0].update(cash_per_share="0", capital_reserve_ratio="0.1", listing_date="2025-06-11")
    # Replacement now refers to the issuer PDF, while its supporting evidence
    # must retain the exact supplier original that created the pending row.
    pdf = tmp_path / "issuer.pdf"; pdf.write_bytes(b"issuer original")
    png = tmp_path / "issuer.png"; png.write_bytes(b"rendered original")
    events[0]["source_response_sha256"] = audit.sha(pdf)
    patch = {"instrument": "SH600000", "ex_date": "2025-06-11", "operation": "replace",
        "issuer_actual_terms_verified": True, "original_pdf_visual_reviewed": True,
        "values": {"capital_reserve_ratio": ".1"},
        "evidence": {**audit._proof(pdf), "url": "https://issuer.example/implementation.pdf"},
        "supporting_evidence": [audit._proof(original), audit._proof(png)]}
    audit._close_normalization_pending(events, [receipt], {("SH600000", "2025-06-11", "capital_reserve_ratio"): ".1"}, [patch])
    assert receipt["unresolved_source_rows"] == [] and len(receipt["resolved_source_rows"]) == 1
    assert "source_normalization_pending" not in events[0]


def test_exact_resolution_binds_every_visual_byte(tmp_path):
    rows = [source_row(), source_row(dividCashPsAfterTax=".3888")]
    root, original = archive(tmp_path, rows); path = fact(tmp_path, root, original, rows)
    (tmp_path / "issuer_page.png").write_bytes(b"changed")
    with pytest.raises(ValueError, match="evidence changed"):normalize(root, resolution_paths=[path])


def strict_capture_fixture(tmp_path):
    root, original = archive(tmp_path, [source_row()])
    manifest = json.loads((root / "collection.json").read_text())
    payload = json.loads(original.read_text()); request = payload["request"]
    guard = {"request": request, "server_echo": request, "status": "verified", "server_error_code": "0",
        "rows_verified": 1, "complete_row_iteration_observed": True,
        "started_at": "2026-10-20T00:59:58+00:00", "finished_at": "2026-10-20T00:59:59+00:00"}
    guard_path = tmp_path / "guard.json"; audit._save(guard_path, guard)
    receipt = {"kind": "a5_complete_new_generation_strict_capture_guard_receipt",
        "status": "capture_identity_iteration_verified_not_a_market_audit_pass", "sources": [{
        "kind": "dividend", "original_collection_manifest": audit._proof(root / "collection.json"),
        "source_generation": manifest["generation"], "as_of_date": manifest["as_of_date"], "response_count": 1,
        "entries": [{"key": "SH600000_2025", "original_response": audit._proof(original),
            "request": request, "strict_server_echo_guard": audit._proof(guard_path)}]}]}
    receipt_path = tmp_path / "strict_capture.json"; audit._save(receipt_path, receipt)
    return root, guard_path, receipt_path, receipt


def test_capture_binds_every_new_original_and_guard_and_discloses_unproven_legacy_empty(tmp_path):
    root, _, receipt_path, _ = strict_capture_fixture(tmp_path)
    report, proofs = audit.verify_stability_capture_receipts([receipt_path], [root / "collection.json"])
    assert report["status"] == "passed" and report["all_sources_single_atomic_supplier_snapshot_proven"] is False
    assert any(p["path"].endswith("guard.json") for p in proofs.values())
    blocked, _ = audit.verify_stability_capture_receipts([], [root / "collection.json"])
    assert blocked["status"] == "blocked"


@pytest.mark.parametrize("mutation", ["year", "iteration", "source_kind", "guard_bytes", "missing_echo"])
def test_capture_rejects_wrong_echo_incomplete_iteration_kind_and_changed_guard(tmp_path, mutation):
    root, guard_path, receipt_path, receipt = strict_capture_fixture(tmp_path)
    guard = json.loads(guard_path.read_text())
    if mutation == "year":guard["server_echo"] = {**guard["request"], "year": "2026"}
    if mutation == "iteration":guard["complete_row_iteration_observed"] = False
    if mutation == "missing_echo":guard["server_echo"] = {}
    if mutation == "source_kind":receipt["sources"][0]["kind"] = "execution"
    if mutation == "guard_bytes":guard["unexpected_mutation"] = True
    audit._save(guard_path, guard)
    if mutation != "guard_bytes":receipt["sources"][0]["entries"][0]["strict_server_echo_guard"] = audit._proof(guard_path)
    audit._save(receipt_path, receipt)
    with pytest.raises(ValueError):audit.verify_stability_capture_receipts([receipt_path], [root / "collection.json"])


def test_same_date_source_events_remain_distinct_and_blocked_in_preflight_never_formal(tmp_path):
    root, _ = archive(tmp_path, [source_row(), source_row(dividCashPsBeforeTax=".2")])
    events, _ = normalize(root)
    with pytest.raises(ValueError, match="Ambiguous duplicate"):
        audit._distribution_tax(events, {"events": []}, {"events": []})
    annotated, report = audit._distribution_tax(events, {"events": []}, {"events": []}, source_preflight_only=True)
    assert annotated == events and len(annotated) == 2
    assert report["status"] == "blocked" and len(report["pending"]) == 2
    assert report["source_ambiguity_groups"][0]["combined_or_selected_event_claimed"] is False


def component_fixture(tmp_path):
    rows = [source_row(), source_row(dividCashPsBeforeTax=".2")]
    root, original = archive(tmp_path, rows); events, _ = normalize(root)
    pdf = tmp_path / "combined.pdf"; pdf.write_bytes(b"issuer expressly combines two fiscal components")
    png = tmp_path / "combined_p1.png"; png.write_bytes(b"reviewed issuer page")
    event = {"instrument": "SH600000", "ex_date": "2025-06-11", "record_date": "2025-06-10", "pay_date": "2025-06-11",
        "listing_date": None, "known_at": "2025-06-01T23:59:59+08:00", "cash_per_share": ".632",
        "stock_dividend_ratio": "0", "capital_reserve_ratio": "0", "bonus_ratio": "0", "cash_dividend_tax_rate": ".20",
        "source_response_sha256": audit.sha(pdf), "source": "issuer_explicit_combined_implemented_cash_distribution"}
    fact = {"instrument": "SH600000", "ex_date": "2025-06-11", "status": "verified_issuer_explicit_combined_cash_distribution",
        "original_pdf_visual_reviewed": True, "issuer_actual_terms_verified": True, "issuer_explicitly_combines_components": True,
        "component_sum_matches_issuer_explicit_total": True, "economic_events_after_resolution": 1,
        "original_source_bytes_modified": False, "original_response": audit._proof(original),
        "issuer_explicit_total_cash_per_share": ".632", "issuer_component_terms": [{"cash_per_share": ".432"}, {"cash_per_share": ".2"}],
        "original_component_rows": [{"row_index_zero_based": n, "row": row, "canonical_row_json_sha256": audit._canonical_row_sha(row)}
            for n, row in enumerate(rows)], "canonical_issuer_event": event,
        "evidence": [{**audit._proof(pdf), "url": "https://issuer.example/combined.pdf"}, audit._proof(png)]}
    path = tmp_path / "component_resolution.json"
    audit._save(path, {"kind": "a5_issuer_verified_component_cash_distribution_resolution",
        "status": "source_component_aggregation_verified_not_full_market_gate", "events": [fact]})
    return events, original, path


def test_only_exact_explicit_issuer_cash_components_create_one_derived_event(tmp_path):
    events, original, path = component_fixture(tmp_path); before = original.read_bytes()
    derived, receipts, proofs = audit.resolve_component_cash_distributions(events, [path], available_originals=[audit._proof(original)])
    assert original.read_bytes() == before and len(derived) == 1
    assert derived[0]["cash_per_share"] == ".632" and derived[0]["event_id"].startswith("issuer_combined:")
    assert set(derived[0]["original_component_event_ids"]) == {e["event_id"] for e in events}
    assert receipts[0]["source_bytes_modified"] is False
    assert any(p["path"].endswith("combined_p1.png") for p in proofs.values())


@pytest.mark.parametrize("mutation", ["index", "total", "implicit", "source", "tax", "third_event", "visual"])
def test_component_resolution_cannot_generalize_sum_change_tax_or_select_rows(tmp_path, mutation):
    events, original, path = component_fixture(tmp_path)
    document = json.loads(path.read_text()); fact = document["events"][0]
    if mutation == "index":fact["original_component_rows"][1]["row_index_zero_based"] = 0
    if mutation == "total":fact["issuer_explicit_total_cash_per_share"] = ".6"
    if mutation == "implicit":fact["issuer_explicitly_combines_components"] = False
    if mutation == "source":fact["canonical_issuer_event"]["source_response_sha256"] = "a" * 64
    if mutation == "tax":fact["canonical_issuer_event"]["cash_dividend_tax_rate"] = ".1"
    if mutation == "third_event":events.append({**events[0], "event_id": "unregistered"})
    if mutation == "visual":(tmp_path / "combined_p1.png").write_bytes(b"changed")
    audit._save(path, document)
    with pytest.raises(ValueError):
        audit.resolve_component_cash_distributions(events, [path], available_originals=[audit._proof(original)])


def rounded_component_fixture(tmp_path):
    _, original, path = component_fixture(tmp_path)
    payload = json.loads(original.read_text()); field = payload["fields"].index("dividCashPsBeforeTax")
    payload["rows"][0][field] = ".4319999"; audit._save(original, payload)
    manifest_path = original.parent / "collection.json"; manifest = json.loads(manifest_path.read_text())
    manifest["responses"]["SH600000_2025"]["sha256"] = audit.sha(original); audit._save(manifest_path, manifest)
    events, _ = normalize(original.parent)
    document = json.loads(path.read_text()); fact = document["events"][0]
    fact.update(component_resolution_method="issuer_explicit_total_replaces_exact_bound_provider_rounded_components",
        component_sum_matches_issuer_explicit_total=False, provider_component_values_are_not_issuer_adjusted_components=True,
        issuer_adjusted_components_not_individually_published=True, provider_component_sum_exact=".6319999",
        issuer_total_minus_provider_component_sum_exact=".0000001", original_response=audit._proof(original),
        issuer_total_derivation={"original_cash_per_share": "1", "entitled_shares": "632", "total_shares": "1000",
            "per10_decimal_places": 6, "rounding": "ROUND_DOWN", "issuer_adjusted_cash_per10": "6.320000"})
    fact.pop("issuer_component_terms")
    row = dict(zip(payload["fields"], payload["rows"][0]))
    fact["original_component_rows"][0].update(row=row, canonical_row_json_sha256=audit._canonical_row_sha(row))
    audit._save(path, document)
    return events, original, path


def test_issuer_total_replacement_requires_exact_formula_and_preserves_nonzero_vendor_difference(tmp_path):
    events, original, path = rounded_component_fixture(tmp_path)
    derived, receipts, _ = audit.resolve_component_cash_distributions(events, [path], available_originals=[audit._proof(original)])
    assert derived[0]["cash_per_share"] == ".632"
    assert receipts[0]["provider_component_sum_exact"] == "0.6319999"
    assert receipts[0]["issuer_total_minus_provider_component_sum_exact"] == "1E-7"


@pytest.mark.parametrize("mutation", ["difference", "invented_components", "rounding", "formula", "generic_method"])
def test_explicit_rounded_component_branch_is_no_general_numeric_tolerance(tmp_path, mutation):
    events, original, path = rounded_component_fixture(tmp_path)
    document = json.loads(path.read_text()); fact = document["events"][0]
    if mutation == "difference":fact["issuer_total_minus_provider_component_sum_exact"] = ".0000002"
    if mutation == "invented_components":fact["issuer_component_terms"] = [{"cash_per_share": ".432"}, {"cash_per_share": ".2"}]
    if mutation == "rounding":fact["issuer_total_derivation"]["rounding"] = "ROUND_HALF_UP"
    if mutation == "formula":fact["issuer_total_derivation"]["entitled_shares"] = "631"
    if mutation == "generic_method":fact.pop("component_resolution_method")
    audit._save(path, document)
    with pytest.raises(ValueError):
        audit.resolve_component_cash_distributions(events, [path], available_originals=[audit._proof(original)])


@pytest.mark.parametrize("mutation", [None, "canonical", "no_fact", "page", "source_pdf"])
def test_issuer_gross_cash_floor_requires_exact_matching_original_page_three_only(tmp_path, mutation):
    events, original, path = rounded_component_fixture(tmp_path)
    document = json.loads(path.read_text()); fact = document["events"][0]
    fact["issuer_cash_payment_gross_rounding"] = "ROUND_DOWN"
    fact["canonical_issuer_event"]["cash_payment_gross_rounding"] = "ROUND_DOWN"
    fact["evidence"][0]["pdf_pages_1based"] = [1, 2, 3]
    fact["evidence"][1].update(pdf_page_1based=3, source_pdf_sha256=fact["canonical_issuer_event"]["source_response_sha256"])
    if mutation == "canonical":fact["canonical_issuer_event"]["cash_payment_gross_rounding"] = "ROUND_HALF_UP"
    if mutation == "no_fact":fact.pop("issuer_cash_payment_gross_rounding")
    if mutation == "page":fact["evidence"][1]["pdf_page_1based"] = 2
    if mutation == "source_pdf":fact["evidence"][1]["source_pdf_sha256"] = "a" * 64
    audit._save(path, document)
    if mutation is None:
        derived, _, _ = audit.resolve_component_cash_distributions(events, [path], available_originals=[audit._proof(original)])
        assert derived[0]["cash_payment_gross_rounding"] == "ROUND_DOWN"
        assert derived[0]["cash_payment_gross_rounding_source_page_1based"] == 3
    else:
        with pytest.raises(ValueError, match="cash truncation"):
            audit.resolve_component_cash_distributions(events, [path], available_originals=[audit._proof(original)])


def test_source_queue_exposes_coverage_availability_and_all_seven_field_failures():
    raw = pd.DataFrame(columns=["date", "tradestatus"])
    queue = audit._stability_source_queue(raw, [], {"unexplained": []}, {"failures": []}, {"failures": []},
        {"pending": [], "tax_classification_unresolved": []}, [], start="2025-01-01", end="2026-10-19",
        coverage={"missing_listed_security_sessions": [{"instrument": "SH600000", "session": "2025-01-02"}],
                  "invalid_trading_sessions": [{"instrument": "SH600000", "reason": "incomplete_trading_quote"}]},
        availability={"mismatches": [{"instrument": "SH600000", "reason": "trading_source_missing_model_bar"}]},
        bridge={"failures": [{"instrument": "SH600000", "reason": "missing_finite_model_feature_fields"}]})
    assert queue["item_count"] == 4
    assert {item.get("source_check") for item in queue["items"]} >= {"source_model_availability", "raw_model_seven_field_bridge"}


def code_clock_fixture(tmp_path):
    root = tmp_path / "scope"; root.mkdir()
    pdf = tmp_path / "code_change.pdf"; pdf.write_bytes(b"official issuer code effective notice")
    event = {"id": "code:SZ300114:2025-02-17", "event_type": "same_issuer_security_code_change",
        "old_instrument": "SZ300114", "new_instrument": "SZ302132", "effective_on": "2025-02-17",
        "first_effective_session": "2025-02-17", "known_at": "2025-02-15T23:59:59+08:00", "price_mapping_allowed": False,
        "evidence": [{**audit._proof(pdf), "url": "https://issuer.example/code_change.pdf"}]}
    audit._save(root / "events.json", {"kind": "ashare_official_index_adjustment_events", "price_mapping_allowed": False, "events": [event]})
    audit._save(root / "manifest.json", {"fixture": True})
    calendar = ["2025-01-02", "2025-02-17", "2025-02-18"]
    raw = pd.DataFrame([{"instrument": "SZ302132", "code": "sz.302132", "date": day,
        "open": "10", "high": "10", "low": "10", "close": "10", "preclose": "10", "volume": "100", "amount": "1000",
        "tradestatus": "1", "isST": "0", "source_complete_quote": True, "source_response_sha256": "a" * 64}
        for day in calendar])
    basic = pd.DataFrame([{"code": "sz.302132", "ipoDate": "2010-08-27", "outDate": ""}])
    return root, raw, basic, calendar


def test_original_code_clock_excludes_supplier_alias_without_rewriting_issuer_ipo_or_prices(tmp_path):
    root, raw, basic, calendar = code_clock_fixture(tmp_path)
    original = raw.copy(deep=True)
    view, receipt, proofs = audit.apply_official_code_validity_view(raw, root, calendar)
    pd.testing.assert_frame_equal(raw, original)
    assert view.date.tolist() == ["2025-02-17", "2025-02-18"]
    assert receipt["excluded_row_count"] == 1 and receipt["excluded_alias_rows"][0]["date"] == "2025-01-02"
    assert receipt["old_code_price_mapping_allowed"] is False and proofs
    result = audit.audit_code_valid_daily_coverage(view, basic, calendar, ["SZ302132"],
        start="2025-01-01", end="2025-02-18", code_view=receipt)
    assert result["status"] == "passed" and basic.iloc[0].ipoDate == "2010-08-27"


def test_model_code_clock_requires_all_seven_fields_unavailable_before_original_code(tmp_path, monkeypatch):
    root, raw, _, calendar = code_clock_fixture(tmp_path)
    _, receipt, _ = audit.apply_official_code_validity_view(raw, root, calendar)
    frame = pd.DataFrame(np.nan, index=calendar, columns=["open", "high", "low", "close", "volume", "vwap", "factor"])
    frame.loc["2025-02-17":] = 10
    monkeypatch.setattr(audit, "read_qlib_security", lambda *a: frame)
    result = audit.audit_model_code_validity("unused", ["SZ302132"], start="2025-01-01", end="2025-02-18", code_view=receipt)
    assert result["status"] == "passed"
    frame.loc["2025-01-02", "factor"] = 1
    result = audit.audit_model_code_validity("unused", ["SZ302132"], start="2025-01-01", end="2025-02-18", code_view=receipt)
    assert result["status"] == "blocked" and result["failures"][0]["session"] == "2025-01-02"


@pytest.mark.parametrize("mutation", ["price_mapping", "late_clock", "wrong_session", "changed_original"])
def test_code_clock_cannot_authorize_price_mapping_guess_session_or_ignore_changed_original(tmp_path, mutation):
    root, raw, _, calendar = code_clock_fixture(tmp_path)
    document = json.loads((root / "events.json").read_text()); event = document["events"][0]
    if mutation == "price_mapping":event["price_mapping_allowed"] = True
    if mutation == "late_clock":event["known_at"] = "2025-02-17T10:00:00+08:00"
    if mutation == "wrong_session":event["first_effective_session"] = "2025-02-18"
    if mutation == "changed_original":(tmp_path / "code_change.pdf").write_bytes(b"changed")
    audit._save(root / "events.json", document)
    with pytest.raises(ValueError):audit.apply_official_code_validity_view(raw, root, calendar)


def build_fixture(tmp_path, monkeypatch, *, unresolved=False, capture_passed=True):
    snapshot_root = tmp_path / "snapshot"; dataset = snapshot_root / "dataset"; dataset.mkdir(parents=True)
    (dataset / "calendars").mkdir(); (dataset / "calendars/day.txt").write_text("2024-12-31\n2025-01-02\n2026-10-19\n")
    audit._save(snapshot_root / "manifest.json", {"fixture": True})
    scope_root = tmp_path / "scope"; scope_root.mkdir(); audit._save(scope_root / "manifest.json", {"scope": True})
    scope = SimpleNamespace(source_receipts=lambda: {"manifest": audit._proof(scope_root / "manifest.json")}, assert_unchanged=lambda: None)
    snap = SimpleNamespace(path=snapshot_root, dataset_dir=dataset)
    raw = pd.DataFrame([{"instrument": "SH600000", "code": "sh.600000", "date": "2025-01-02", "tradestatus": "1",
        "open": "10", "high": "10", "low": "10", "close": "10", "preclose": "10",
        "volume": "100", "amount": "1000", "adjustflag": "3", "isST": "0",
        "source_response_sha256": "a" * 64}])
    evidence = audit.A5StabilityMarketEvidence(raw, pd.DataFrame([{"code": "sh.600000"}]), [], ["SH600000"], [], {}, [], [],
        {"capture_identity_audit": {"status": "passed" if capture_passed else "blocked"}},
        [{"unresolved_source_rows": [{"instrument": "SH600000", "ex_date": "2025-01-02", "reasons": ["missing"]}]}] if unresolved else [])
    monkeypatch.setattr(audit, "load_qlib_snapshot", lambda *a, **k: snap)
    monkeypatch.setattr(audit, "load_stability_market_evidence", lambda *a, **k: evidence)
    monkeypatch.setattr(audit, "apply_official_code_validity_view", lambda raw, *a, **k: (raw,
        {"status": "passed", "validity_intervals": {}}, {}))
    monkeypatch.setattr(audit, "audit_official_membership_evidence", lambda *a, **k: ({"status": "passed"}, {}))
    for name in ["audit_daily_coverage", "audit_member_identity", "audit_qlib_availability"]:
        monkeypatch.setattr(audit, name, lambda *a, **k: {"status": "passed", "failures": []})
    monkeypatch.setattr(audit, "audit_raw_qlib_bridge", lambda *a, **k: {"status": "passed", "partial_model_score_exceptions": []})
    monkeypatch.setattr(audit, "audit_temporal_adjustments", lambda *a, **k: {"status": "passed", "failures": [], "source_rule": audit._proof(audit.DEFAULT_SPEC)})
    monkeypatch.setattr(audit, "audit_reference_prices", lambda *a, **k: {"status": "passed", "unexplained": []})
    return snapshot_root, scope_root, scope


@pytest.mark.parametrize("formal,unresolved,capture_passed,expected", [
    (False, False, True, "blocked"), (True, False, True, "passed"),
    (True, True, True, "blocked"), (True, False, False, "blocked")])
def test_secondary_kind_contract_binding_and_formal_gate_are_independent_of_primary(
        tmp_path, monkeypatch, formal, unresolved, capture_passed, expected):
    snapshot, scope_path, scope = build_fixture(tmp_path, monkeypatch, unresolved=unresolved, capture_passed=capture_passed)
    result = audit.build_a5_stability_market_audit(snapshot, scope_path, [], [], tmp_path / "output",
        start="2025-01-01", end="2026-10-19", historical_scope=scope, archive_scope_view=True,
        source_preflight_only=not formal)
    assert result["kind"] == "a5_stability_market_audit" and result["status"] == expected
    assert len(result["checks"]) == 13 and "distribution_tax_accounting" in result["checks"]
    contracts = result["bound_inputs"]["distribution_tax_contracts"]
    assert audit._proof(contracts["path"]) == contracts
    assert json.loads(Path(contracts["path"]).read_text())["contracts"] == []
    assert result["portfolio_returns_computed"] is False
