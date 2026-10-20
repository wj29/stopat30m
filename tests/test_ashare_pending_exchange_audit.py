"""Synthetic accounting tests; no real experiment or model is executed."""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json

import pytest

from stopat30m.ashare_pending_exchange_audit import (
    _conditional_metrics,
    _verify_manifest,
    value_pending_share_exchange_overlay,
)


OLD, NEW = "SH601059", "SH601995"
DAY, RECORD = "2026-10-19", "2026-10-16"


@pytest.fixture
def inputs(tmp_path):
    source = tmp_path / "synthetic-reviewed-issuer-evidence.pdf"
    source.write_bytes(b"Synthetic issuer evidence fixture; not a real PDF or announcement")
    quantity = 300
    old_value = Decimal(quantity) * Decimal("15.56")
    equity = Decimal("90000") + old_value + Decimal("3000") + Decimal("200")
    curve = dict(date=DAY, equity_cny=str(equity), equity_upper_bound_cny=str(equity),
                 equity_with_claim_estimate_cny=str(equity), cash_cny="90000.00",
                 nav=float(equity / 100000), net_return=float(equity / 100000 - 1),
                 accounting_valid=False, valuation_complete=False)
    positions = [
        dict(session=DAY, instrument=OLD, quantity=quantity, price="15.5600",
             price_date="2026-09-14", market_value=str(old_value),
             valuation_source="carried_observed_mark"),
        dict(session=DAY, instrument=NEW, quantity=100, price="30.00",
             price_date=DAY, market_value="3000.00", valuation_source="observed_raw_close"),
    ]
    state = dict(schema_version=1, session=DAY, cash="90000.00",
                 positions={OLD: dict(quantity=quantity, bought_today=0, bought_date="2026-08-13",
                                      last_mark="15.5600", last_mark_date="2026-09-14",
                                      last_mark_basis="observed_raw_close"),
                            NEW: dict(quantity=100, bought_today=0, bought_date="2026-07-01",
                                      last_mark="30.00", last_mark_date=DAY,
                                      last_mark_basis="observed_raw_close")},
                 holdings_history={RECORD: {OLD: quantity, NEW: 100}, DAY: {OLD: quantity, NEW: 100}},
                 claims={}, applied_actions={}, entitlements={
                     "old_cash_dividend": dict(kind="cash_dividend", instrument=OLD,
                                               status="pending", net_cash="200.00",
                                               record_date="2026-09-10", pay_date="2026-10-20")})
    contract = dict(kind="a7_verified_pending_share_exchange", id="synthetic-cinda-exchange",
                    from_instrument=OLD, to_instrument=NEW, ratio="0.5210", record_date=RECORD,
                    record_date_known_at="2026-10-16T23:59:59+08:00",
                    delisting_date=DAY, delisting_known_at="2026-10-13T23:59:59+08:00",
                    ratio_known_at="2026-10-16T23:59:59+08:00",
                    shareholder_class="ordinary_public_non_locked_no_cash_option_exercise",
                    allocation_policy="account_aggregate_floor_or_ceil_largest_remainder",
                    completion_verified=False, tradability_verified=False,
                    facts_verified=["record_date", "ratio", "delisting", "allocation_policy", "pending_completion"],
                    evidence={"issuer": dict(path=str(source), sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                                             url="https://static.cninfo.com.cn/synthetic-unit-test/notice.pdf",
                                             original_pdf_visual_reviewed=True)})
    quote = dict(instrument=NEW, session=DAY, close="30.00", tradestatus="1", source_sha256="a" * 64)
    return dict(session=DAY, base_curve=curve, base_positions=positions, final_state=state,
                contract=contract, successor_quote=quote)


def set_quantity(case, quantity):
    """Change the synthetic old holding while preserving all NAV components."""
    old = case["final_state"]["positions"][OLD]["quantity"]
    delta = Decimal(quantity - old) * Decimal("15.56")
    case["final_state"]["positions"][OLD]["quantity"] = quantity
    for snapshot in case["final_state"]["holdings_history"].values():
        snapshot[OLD] = quantity
    case["base_positions"][0]["quantity"] = quantity
    case["base_positions"][0]["market_value"] = str(Decimal(quantity) * Decimal("15.56"))
    for name in ("equity_cny", "equity_upper_bound_cny", "equity_with_claim_estimate_cny"):
        case["base_curve"][name] = str(Decimal(case["base_curve"][name]) + delta)
    equity = Decimal(case["base_curve"]["equity_cny"])
    case["base_curve"].update(nav=float(equity / 100000), net_return=float(equity / 100000 - 1))


@pytest.mark.parametrize("quantity,lower,upper,exact", [(200, 104, 105, "104.2000"),
                                                       (300, 156, 157, "156.3000"),
                                                       (1000, 521, 521, "521.0000")])
def test_account_aggregate_quantity_bounds_replace_exactly_one_stale_mark(inputs, quantity, lower, upper, exact):
    set_quantity(inputs, quantity)
    before = deepcopy(inputs)
    result = value_pending_share_exchange_overlay(**inputs)
    assert inputs == before
    curve, rights, audit = result["curve"], result["rights"], result["audit"]
    assert len(rights) == 1
    assert Decimal(rights[0]["quantity_lower"]) == lower
    assert Decimal(rights[0]["quantity_upper"]) == upper
    assert Decimal(rights[0]["exact_successor_quantity"]) == Decimal(exact)
    # Existing successor100 shares and old cash-dividend200 remain independent.
    unchanged_value = Decimal("90000") + Decimal("3000") + Decimal("200")
    assert Decimal(curve["equity_cny"]) == unchanged_value + lower * 30
    assert Decimal(curve["equity_upper_bound_cny"]) == unchanged_value + upper * 30
    assert Decimal(curve["equity_with_claim_estimate_cny"]) == unchanged_value + Decimal(exact) * 30
    assert Decimal(curve["cash_cny"]) == Decimal("90000")
    assert Decimal(audit["old_mark_removed_cny"]) == quantity * Decimal("15.56")
    assert audit["economic_rights_accounting_bounds_valid"] is True
    assert audit["performance_valid"] is False
    assert audit["settlement_assumed"] is False and audit["tradable"] is False
    assert Decimal(audit["unconditional_recovery_floor_cny"]) == 0


def test_repeat_same_frozen_inputs_is_deterministic_and_does_not_double_count(inputs):
    one = value_pending_share_exchange_overlay(**inputs)
    two = value_pending_share_exchange_overlay(**inputs)
    assert one == two
    assert inputs["final_state"]["positions"][NEW]["quantity"] == 100
    assert inputs["final_state"]["entitlements"]["old_cash_dividend"]["net_cash"] == "200.00"


def test_already_adjusted_curve_cannot_remove_same_old_value_a_second_time(inputs):
    result = value_pending_share_exchange_overlay(**inputs)
    inputs["base_curve"] = result["curve"]
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_state_session_cannot_be_mismatched_with_terminal_valuation(inputs):
    inputs["final_state"]["session"] = RECORD
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_conditional_drawdown_uses_upper_running_peak_and_lower_current_nav():
    curves = [dict(equity_cny="100.00", equity_upper_bound_cny="200.00"),
              dict(equity_cny="99.00", equity_upper_bound_cny="100.00")]
    result = _conditional_metrics(curves, "100.00", "0.00")
    # A lower-only peak would understate this as -1%; the interval bound is -50.5%.
    assert Decimal(result["conditional_proxy_conservative_max_drawdown"]) == Decimal("-.505")
    assert Decimal(result["conditional_proxy_net_return_lower"]) == Decimal("-.01")
    assert Decimal(result["conditional_proxy_net_return_upper"]) == 0
    assert result["performance_valid"] is False


@pytest.mark.parametrize("ratio", ["0", "-1", "NaN", "not-a-ratio", True])
def test_invalid_ratio_never_creates_pending_equity(inputs, ratio):
    inputs["contract"]["ratio"] = ratio
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_issuer_fact_coverage_cannot_be_replaced_by_generic_review_flag(inputs):
    inputs["contract"]["facts_verified"].remove("allocation_policy")
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_terminal_before_event_preserves_original_curve_without_future_record_snapshot(inputs):
    inputs["session"] = "2025-12-31"
    inputs["base_curve"]["date"] = "2025-12-31"
    inputs["final_state"]["holdings_history"] = {"2025-12-31": {OLD: 300}}
    result = value_pending_share_exchange_overlay(**inputs)
    assert result["curve"] == inputs["base_curve"]
    assert not result["rights"]


def test_zero_record_and_terminal_exposure_preserves_original_curve(inputs):
    set_quantity(inputs, 0)
    inputs["final_state"]["positions"].pop(OLD)
    inputs["base_positions"] = [row for row in inputs["base_positions"] if row["instrument"] != OLD]
    inputs["base_curve"].update(accounting_valid=True, valuation_complete=True)
    result = value_pending_share_exchange_overlay(**inputs)
    assert result["curve"] == inputs["base_curve"]
    assert not result["rights"]


def test_eligibility_requires_finalized_record_snapshot_not_terminal_shares(inputs):
    del inputs["final_state"]["holdings_history"][RECORD]
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_record_to_terminal_stock_delta_cannot_be_guessed(inputs):
    inputs["final_state"]["holdings_history"][RECORD][OLD] = 200
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


@pytest.mark.parametrize("kind", ["pending_bonus", "fractional_claim"])
def test_overlapping_stock_rights_require_independent_allocation_policy(inputs, kind):
    if kind == "pending_bonus":
        inputs["final_state"]["entitlements"]["overlap"] = dict(
            kind="bonus_shares", instrument=OLD, status="pending", quantity="0.4")
    else:
        inputs["final_state"]["claims"]["overlap"] = dict(instrument=OLD, quantity="0.4")
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


@pytest.mark.parametrize("field", ["record_date_known_at", "delisting_known_at", "ratio_known_at"])
def test_future_knowledge_is_not_imported_into_terminal_valuation(inputs, field):
    inputs["contract"][field] = "2026-10-19T15:00:01+08:00"
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


@pytest.mark.parametrize("change", [dict(instrument=OLD), dict(session="2026-10-16"),
                                    dict(tradestatus="0"), dict(close="0"), dict(close="NaN"),
                                    dict(close="-1"), dict(source_sha256="unknown")])
def test_successor_requires_same_day_positive_original_trading_quote(inputs, change):
    inputs["successor_quote"].update(change)
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_missing_successor_quote_is_not_a_license_to_carry_old_price(inputs):
    inputs["successor_quote"] = None
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


def test_changed_original_source_bytes_are_rejected_even_on_repeat_call(inputs):
    value_pending_share_exchange_overlay(**inputs)
    from pathlib import Path
    Path(inputs["contract"]["evidence"]["issuer"]["path"]).write_bytes(b"changed evidence")
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


@pytest.mark.parametrize("fault", ["duplicate_old_row", "missing_old_row", "row_quantity", "state_cash", "insufficient_nav"])
def test_base_ledger_reconciliation_fails_closed(inputs, fault):
    if fault == "duplicate_old_row":
        inputs["base_positions"].append(deepcopy(inputs["base_positions"][0]))
    elif fault == "missing_old_row":
        inputs["base_positions"] = [row for row in inputs["base_positions"] if row["instrument"] != OLD]
    elif fault == "row_quantity":
        inputs["base_positions"][0]["quantity"] = 200
    elif fault == "state_cash":
        inputs["final_state"]["cash"] = "1.00"
    else:
        inputs["base_curve"]["equity_cny"] = "1.00"
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


@pytest.mark.parametrize("field,value", [("completion_verified", True), ("tradability_verified", True),
                                        ("allocation_policy", "give_everyone_rounded_up_shares"),
                                        ("shareholder_class", "unknown_cash_option_status")])
def test_pending_adapter_cannot_invent_completion_or_unknown_eligibility(inputs, field, value):
    inputs["contract"][field] = value
    with pytest.raises(ValueError):
        value_pending_share_exchange_overlay(**inputs)


@pytest.mark.parametrize("schema", ["original_files", "files"])
def test_accepts_both_original_capture_and_experiment_manifest_formats(tmp_path, schema):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"explicitly synthetic input bytes")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    if schema == "original_files":
        manifest = {"kind": "a7_official_exchange_source_investigation_manifest",
            "original_files": {source.name: {"path": str(source), "sha256": digest}}}
    else:
        manifest = {"kind": "synthetic_experiment", "files": {source.name: digest}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    assert _verify_manifest(tmp_path) == manifest
    source.write_bytes(b"tampered")
    with pytest.raises(ValueError):
        _verify_manifest(tmp_path)


def test_original_capture_proof_cannot_point_to_a_different_file(tmp_path):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"synthetic")
    manifest = {"kind": "a7_official_exchange_source_investigation_manifest",
        "original_files": {source.name: {"path": str(tmp_path / "other.pdf"),
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="proof points"):
        _verify_manifest(tmp_path)


def test_unsealed_extra_source_file_is_detected(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps({
        "kind": "a7_official_exchange_source_investigation_manifest", "original_files": {}}))
    (tmp_path / "extra.pdf").write_bytes(b"synthetic")
    with pytest.raises(ValueError, match="inventory"):
        _verify_manifest(tmp_path)
