from contextlib import nullcontext
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_frozen_horizon_diagnostics as diag


def context():
    calendar = pd.bdate_range("2025-01-01", periods=36).strftime("%Y-%m-%d").tolist()
    names = [f"SH{600000+number}" for number in range(25)]
    prices = pd.DataFrame({inst: [100+(.2+.03*number)*day+.001*(number%4)*day**2
        for day in range(len(calendar))] for number, inst in enumerate(names)}, index=calendar)
    scores = pd.Series([float(number) for day in calendar for number in range(len(names))],
        index=pd.MultiIndex.from_product([pd.to_datetime(calendar), names], names=["datetime", "instrument"]))
    pool = SimpleNamespace(members_on=lambda day: names, verified_batch=nullcontext, assert_unchanged=lambda: None)
    return dict(calendar=calendar, normalized_open=prices, scores={"LGB": scores}, pool=pool,
        protocol={"periods": {period: [calendar[1], calendar[-1]] for period in diag.PERIODS}},
        bound={}, actions=SimpleNamespace(assert_unchanged=lambda: None))


def test_native_five_session_full_dictionary_reproduces_original_in_all_periods():
    data = context()
    result = diag.diagnose_frozen_horizons(data)
    for period in diag.PERIODS:
        assert result[period]["native"]["5"] == diag.stability.score_quality(data, model="LGB", period=period)
        assert result[period]["all_three_original_five_session_dicts_reproduced_before_other_horizons"]


def test_entry_delay_and_horizon_maturity_use_actual_calendar_indices():
    data = context()
    # Synthetic calendar deliberately removes a nominal business day.
    removed = data["calendar"].pop(5)
    data["normalized_open"] = data["normalized_open"].drop(index=removed)
    data["scores"]["LGB"] = data["scores"]["LGB"].drop(index=pd.Timestamp(removed), level="datetime")
    for bounds in data["protocol"]["periods"].values():
        bounds[1] = data["calendar"][-1]
    output = diag.diagnose_frozen_horizons(data)["2025"]
    for h in diag.HORIZONS:
        result = output["native"][str(h)]
        assert result["summary"]["excluded"]["immature_signal_days"] == h
        assert result["daily"][0]["signal_date"] == data["calendar"][0]
        assert result["daily"][0]["label_end"] == data["calendar"][h+1]
        assert result["summary"]["label"] == f"Ref($open,-{h+1})/Ref($open,-1)-1"


def test_matched_comparison_has_identical_dates_and_stock_endpoint_intersection():
    data = context()
    first, inst = data["calendar"][0], data["normalized_open"].columns[0]
    # The10-day endpoint is absent; the same stock still has a valid5-day endpoint.
    data["normalized_open"].loc[data["calendar"][11], inst] = np.nan
    result = diag.diagnose_frozen_horizons(data)["2025"]
    assert result["native"]["5"]["daily"][0]["pairs"] == 25
    for h in diag.HORIZONS:
        assert result["matched"][str(h)]["daily"][0]["pairs"] == 24
        assert result["matched"][str(h)]["daily"][0]["signal_date"] == first
    common = result["matched_coverage"][0]
    assert inst not in common["common_instruments"]
    assert common["missing_label_pairs_by_horizon"] == {"5": 0, "10": 1, "20": 0}
    assert common["missing_any_endpoint_pairs"] == 1
    dates = [row["signal_date"] for row in result["matched"]["5"]["daily"]]
    for h in (10, 20):
        assert dates == [row["signal_date"] for row in result["matched"][str(h)]["daily"]]
    assert dates[-1] == data["calendar"][-22]


def test_future_index_exit_is_not_used_to_filter_an_observed_label():
    data = context()
    inst = data["normalized_open"].columns[0]
    calls = []
    def historical(day):
        calls.append(day)
        return list(data["normalized_open"].columns) if day == data["calendar"][0] else list(data["normalized_open"].columns[1:])
    data["pool"].members_on = historical
    result = diag.diagnose_frozen_horizons(data)["2025"]
    assert inst in result["matched_coverage"][0]["common_instruments"]
    assert result["native"]["20"]["daily"][0]["pairs"] == 25
    assert result["future_membership_conditioned"] is False
    assert data["calendar"][-1] not in calls  # No query for execution/label endpoint membership.


def test_constant_one_horizon_excludes_date_from_every_matched_horizon():
    data = context()
    data["normalized_open"].loc[data["calendar"][11]] = data["normalized_open"].loc[data["calendar"][1]].to_numpy()*1.1
    # Exact shared endpoint ratio must be constant, avoiding floating division noise.
    data["normalized_open"].loc[data["calendar"][1]] = 100.
    data["normalized_open"].loc[data["calendar"][11]] = 110.
    output = diag.diagnose_frozen_horizons(data)["2025"]
    assert output["matched_coverage"][0]["constant_horizons"] == [10]
    assert all(data["calendar"][0] not in [row["signal_date"] for row in output["matched"][str(h)]["daily"]] for h in diag.HORIZONS)


def test_five_day_baseline_failure_prevents_any_other_horizon_calculation(monkeypatch):
    data = context(); called = []
    real = diag._native_quality
    def wrapper(*args, **kwargs):
        called.append(kwargs["horizon"])
        return real(*args, **kwargs)
    monkeypatch.setattr(diag, "_native_quality", wrapper)
    bad = {period: {"wrong": "full original dictionary"} for period in diag.PERIODS}
    with pytest.raises(ValueError, match="exact reproduction"):
        diag.diagnose_frozen_horizons(data, expected_five_day=bad)
    assert called == [5]


def test_unknown_horizons_and_missing_periods_are_rejected():
    with pytest.raises(ValueError, match="predeclared"):
        diag._native_quality(context(), period="2025", horizon=15)
    data = context(); data["protocol"]["periods"].pop("2026")
    with pytest.raises(ValueError, match="All three"):
        diag.diagnose_frozen_horizons(data)


def test_missing_endpoints_and_finite_scores_are_disclosed_without_forward_fill():
    data = context()
    first, inst = data["calendar"][0], data["normalized_open"].columns[0]
    data["normalized_open"].loc[data["calendar"][6], inst] = 0
    result = diag.diagnose_frozen_horizons(data)["2025"]
    detail = result["native_coverage"]["5"][0]
    assert detail["signal_date"] == first and detail["missing_label_pairs"] == 1 and detail["observed_label_pairs"] == 24
    data["scores"]["LGB"].loc[(pd.Timestamp(first), inst)] = np.inf
    result = diag.diagnose_frozen_horizons(data)["2025"]
    assert result["native_coverage"]["5"][0]["finite_score_and_price_column_pairs"] == 24


def freeze_fixture(tmp_path, monkeypatch):
    data = context()
    models = tmp_path/"synthetic_models"; models.mkdir()
    (models/"predictions.csv").write_text("explicit synthetic predictions fixture\n")
    (models/"model.txt").write_text("explicit synthetic native model fixture; never loaded\n")
    diag.stability._save(models/"manifest.json", {"files": {name: diag.stability.sha(models/name)
        for name in ("predictions.csv", "model.txt")}})
    source_paths = {}
    for name in ("protocol", "market_audit", "merger_actions"):
        p=tmp_path/(name+".json");diag.stability._save(p,{"synthetic_fixture_only": True});source_paths[name]=diag.stability._proof(p)
    bundle=tmp_path/"bundle.json";diag.stability._save(bundle, {"snapshot_path": str(tmp_path/"synthetic_snapshot"), "scope_path": str(tmp_path/"synthetic_scope")})
    source_paths["source_bundle"]=diag.stability._proof(bundle)
    turnover=tmp_path/"turnover_report.json";diag.stability._save(turnover,{"kind": "a6_fixed_lgb_turnover_results", "bound_inputs": source_paths})
    design = {**deepcopy(diag.DESIGN_TERMS), "declared_at": "2026-10-20T00:00:00+00:00"}
    designpath=tmp_path/"design.json";diag.stability._save(designpath, design)
    score_proofs={}
    for period in diag.PERIODS:
        p=tmp_path/(period+"_score_quality.json");diag.stability._save(p,diag.stability.score_quality(data,model="LGB",period=period));score_proofs[period]=diag.stability._proof(p)
    frozen={"kind": "a7_preimplementation_horizon_design_freeze", "design": design,
        "no_new_fit_or_parameters_or_forward_strategy_changes": True, "bound_design":diag.stability._proof(designpath),
        "original_predictions":diag.stability._proof(models/"predictions.csv"), "original_models_manifest":diag.stability._proof(models/"manifest.json"),
        "original_turnover_report":diag.stability._proof(turnover), "original_baseline_scores":score_proofs}
    p=tmp_path/"freeze.json";diag.stability._save(p,frozen)
    monkeypatch.setattr(diag,"verify_reused_stability_models",lambda *a,**kw:{"explicit_synthetic_verifier_fixture":True})
    monkeypatch.setattr(diag,"load_qlib_snapshot",lambda *a,**kw:None)
    monkeypatch.setattr(diag.stability,"load_stability_context",lambda *a,**kw:data)
    return p,data


def test_runtime_seal_precedes_correlations_and_all_sets_have_verified_manifest(tmp_path,monkeypatch):
    design,data=freeze_fixture(tmp_path,monkeypatch)
    output=tmp_path/"diagnostics"
    real=diag.diagnose_frozen_horizons
    def guarded(*args,**kwargs):
        seal=json.loads((output/"declaration.json").read_text())
        assert seal["any_horizon_results_computed_before_runtime_declaration"] is False
        assert "horizon_implementation" in seal["bound_inputs"]
        assert "frozen_model:model.txt" in seal["bound_inputs"]
        assert not (output/"report.json").exists()
        return real(*args,**kwargs)
    monkeypatch.setattr(diag,"diagnose_frozen_horizons",guarded)
    report=diag.run_frozen_horizon_diagnostics(design,output)
    assert report["all_nine_native_and_nine_matched_sets_present"]
    assert not report["fitting_performed"] and not report["parameters_selected"]
    assert len(list(output.glob("*_native_h*.json")))==9
    assert len(list(output.glob("*_matched_h*.json")))==9
    manifest=json.loads((output/"manifest.json").read_text())
    assert all(diag.stability.sha(output/name)==digest for name,digest in manifest["files"].items())
    with pytest.raises(ValueError,match="Preserve|immutable"):
        diag.run_frozen_horizon_diagnostics(design,output)


def test_changed_frozen_prediction_is_rejected_before_any_output(tmp_path,monkeypatch):
    design,data=freeze_fixture(tmp_path,monkeypatch)
    p=json.loads(design.read_text())["original_predictions"]["path"]
    Path(p).write_text("tampered predictions")
    with pytest.raises(ValueError,match="source bytes changed"):
        diag.run_frozen_horizon_diagnostics(design,tmp_path/"not_created")
    assert not (tmp_path/"not_created").exists()


def test_design_horizon_or_entry_terms_cannot_change(tmp_path,monkeypatch):
    design,data=freeze_fixture(tmp_path,monkeypatch)
    frozen=json.loads(design.read_text());frozen["design"]["horizons_actual_sessions"]=[5,15,20]
    diag.stability._save(design,frozen)
    with pytest.raises(ValueError,match="preregistered"):
        diag.run_frozen_horizon_diagnostics(design,tmp_path/"not_created")
    assert not (tmp_path/"not_created").exists()


def test_changed_native_model_during_diagnostics_keeps_failure_without_report(tmp_path,monkeypatch):
    design,data=freeze_fixture(tmp_path,monkeypatch)
    native=tmp_path/"synthetic_models/model.txt"
    real=diag.diagnose_frozen_horizons
    def changed(*args,**kwargs):
        result=real(*args,**kwargs)
        native.write_text("changed after runtime seal")
        return result
    monkeypatch.setattr(diag,"diagnose_frozen_horizons",changed)
    output=tmp_path/"failed_diagnostics"
    with pytest.raises(ValueError,match="source bytes changed"):
        diag.run_frozen_horizon_diagnostics(design,output)
    assert (output/"declaration.json").exists() and (output/"failure.json").exists()
    assert not (output/"report.json").exists() and not (output/"manifest.json").exists()
