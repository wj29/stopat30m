"""A specifically proved partial halt cannot hide a normalized-price seam."""
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from stopat30m.data import a5_adjusted_prices as adjusted
from stopat30m.data import a5_market_audit as market


A = "SH688065"
DAYS = ["2023-06-14", "2023-06-15", "2023-06-26"]


def fixture(tmp_path, monkeypatch, *, normalized=(.5, np.nan, .75)):
    def write(name, value):
        path = tmp_path / name
        path.write_text(json.dumps(value))
        return path
    def proof(path, **kw):
        return {"path": str(path), "sha256": adjusted.sha(path), **kw}
    daily = write("daily.json", {"fields": ["date", "code", "open", "close", "tradestatus"],
        "rows": [[DAYS[1], "sh.688065", "12", "12", "0"]]})
    fact = {"instrument": A, "date": DAYS[1], "source_response_sha256": adjusted.sha(daily),
        "source_observed_open": "12", "source_observed_close": "12", "execution_remains_blocked": True,
        "source_status_unchanged": "0", "halt_period": "from_afternoon_open"}
    intraday = write("intraday.json", {"events": [fact]})
    pdf, image = tmp_path / "issuer.pdf", tmp_path / "issuer_p1.png"
    pdf.write_bytes(b"synthetic original issuer PDF")
    image.write_bytes(b"synthetic original rendered page")
    rule = write("rule.json", {"kind": "a5_specific_partial_session_model_spec",
        "status": "frozen_before_union_model_fit_and_performance", "source_reference_margin_cny": "0.011",
        "chain": "every_original_observed_close_divided_by_its_published_preclose", "model_bar_imputation": False})
    event = {**fact, "source_complete_quote": False, "source_tradestatus": "0",
        "observed_partial_quote_preserved": True, "original_pdf_visual_reviewed": True,
        "allowed_missing_model_fields": list(adjusted.MODEL_FIELDS)}
    policy = write("policy.json", {"kind": "a5_specific_partial_session_model_policy",
        "status": "frozen_before_union_model_fit_and_performance", "model_bars_or_factors_filled": False,
        "complete_quote_exceptions_allowed": False, "portfolio_returns_consulted": False,
        "events": [event], "bound_inputs": {"original_daily": proof(daily), "intraday_observations": proof(intraday),
            "issuer_pdf": proof(pdf, url="https://issuer.example/original.pdf", pdf_pages_1based=[1]),
            "reviewed_original_page": proof(image), "rule_spec": proof(rule)}})
    spec = json.loads((adjusted.DEFAULT_SPEC.parent / "a5_adjusted_price_bridge_spec_v2.json").read_text())
    spec["partial_rule_spec"] = proof(rule)
    bridge = write("bridge.json", spec)
    raw = pd.DataFrame({"instrument": A, "date": DAYS, "open": ["10", "12", "15"],
        "high": ["10", "12", "15"], "low": ["10", "12", "15"], "close": ["10", "12", "15"],
        "preclose": ["10", "10", "12"], "tradestatus": ["1", "0", "1"],
        "source_observed_market_quote": True, "source_complete_quote": [True, False, True],
        "source_response_sha256": adjusted.sha(daily)})
    stored = pd.DataFrame({field: np.asarray(normalized, dtype=np.float32) for field in adjusted.MODEL_FIELDS}, index=DAYS)
    for field in ("volume", "vwap", "factor"):
        stored[field] = np.asarray([1, np.nan, 1], dtype=np.float32)
    stored["factor"] = np.asarray([.05, np.nan, .05], dtype=np.float32)
    monkeypatch.setattr(adjusted, "read_qlib_security", lambda *args: stored)
    monkeypatch.setattr(market, "read_qlib_security", lambda *args: stored)
    return raw, stored, policy, bridge, image


def test_complete_source_chain_certifies_anchor_return_without_filling_partial_model_bar(tmp_path, monkeypatch):
    raw, stored, policy, bridge, _ = fixture(tmp_path, monkeypatch)
    strict = adjusted.audit_temporal_adjustments("fixture", raw, [A], start=DAYS[0], end=DAYS[-1])
    assert strict["status"] == "blocked"
    result = adjusted.audit_temporal_adjustments("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        spec_path=bridge, partial_session_model_policy_path=policy)
    assert result["status"] == "passed" and result["checked_partial_source_chains"] == 1
    chain = result["certified_partial_source_chains"][0]
    assert [r["session"] for r in chain["original_observed_source_legs"]] == DAYS[1:]
    assert chain["individually_missing_edges_replaced_by_certified_chain"] == 2
    assert chain["individual_gap_model_return_certified"] is False
    assert stored.loc[DAYS[1]].isna().all()


def test_partial_chain_cannot_hide_a_material_factor_seam(tmp_path, monkeypatch):
    raw, _, policy, bridge, _ = fixture(tmp_path, monkeypatch, normalized=(.5, np.nan, 1))
    result = adjusted.audit_temporal_adjustments("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        spec_path=bridge, partial_session_model_policy_path=policy)
    assert result["status"] == "blocked"
    assert any(r["reason"] == "unexplained_partial_model_chain_return_seam" for r in result["failures"])


def test_original_partial_return_leg_cannot_be_silently_skipped(tmp_path, monkeypatch):
    raw, _, policy, bridge, _ = fixture(tmp_path, monkeypatch)
    raw.loc[raw.date.eq(DAYS[1]), "preclose"] = "8"
    result = adjusted.audit_temporal_adjustments("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        spec_path=bridge, partial_session_model_policy_path=policy)
    assert result["status"] == "blocked"


def test_missing_return_anchor_is_not_excused_by_a_partial_policy(tmp_path, monkeypatch):
    raw, stored, policy, bridge, _ = fixture(tmp_path, monkeypatch)
    stored.loc[DAYS[-1]] = np.nan
    result = adjusted.audit_temporal_adjustments("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        spec_path=bridge, partial_session_model_policy_path=policy)
    assert result["status"] == "blocked"


def test_specific_partial_gap_can_only_excuse_all_seven_nan_fields_of_the_evidenced_quote(tmp_path, monkeypatch):
    raw, stored, policy, _, _ = fixture(tmp_path, monkeypatch)
    result = market.audit_raw_qlib_bridge("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        partial_session_model_policy_path=policy)
    assert result["status"] == "passed" and len(result["partial_model_score_exceptions"]) == 1
    raw.loc[raw.date.eq(DAYS[1]), "source_complete_quote"] = True
    result = market.audit_raw_qlib_bridge("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        partial_session_model_policy_path=policy)
    assert result["status"] == "blocked" and result["partial_model_score_exceptions"] == []
    raw.loc[raw.date.eq(DAYS[1]), "source_complete_quote"] = False
    stored.loc[DAYS[1], "open"] = 0
    assert market.audit_raw_qlib_bridge("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
        partial_session_model_policy_path=policy)["status"] == "blocked"


def test_partial_policy_rechecks_reviewed_page_and_does_not_authorize_unlisted_dates(tmp_path, monkeypatch):
    _, _, policy, _, image = fixture(tmp_path, monkeypatch)
    image.write_bytes(b"changed page")
    with pytest.raises(ValueError, match="original bytes changed"):
        adjusted.load_partial_session_model_policy(policy)


def test_original_v1_temporal_rule_cannot_silently_adopt_the_later_exception(tmp_path, monkeypatch):
    raw, _, policy, _, _ = fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="original temporal rule"):
        adjusted.audit_temporal_adjustments("fixture", raw, [A], start=DAYS[0], end=DAYS[-1],
            partial_session_model_policy_path=policy)


def test_runtime_provenance_is_captured_before_work_and_rejects_midrun_source_mutation(tmp_path):
    code = tmp_path / "runtime.py"
    code.write_text("original implementation")
    captured = market._capture_runtime_proofs({"implementation": code})
    original_sha = captured["implementation"]["sha256"]
    code.write_text("a different implementation during a long source audit")
    assert original_sha != market.sha(code)
    with pytest.raises(ValueError, match="runtime implementation changed"):
        market._assert_bound_inputs_unchanged(captured)
