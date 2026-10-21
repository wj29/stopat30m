from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_model_debug_research as debug
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm, load_fixed_qlib_model


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2)+"\n")
    return debug._proof(path)


def bound_declaration(tmp_path):
    design = Path(__file__).parents[1]/"docs/research/a14_model_debug_design_v1.json"
    families = {}
    for family in ("primary", "stability"):
        families[family] = {key: write(tmp_path/f"{family}_{key}.json", {"input": key})
            for key in ("model_manifest", "protocol", "market_audit", "stock_basic")}
    path = tmp_path/"declaration.json"
    write(path, {"kind": debug.KIND, "schema_version": 1,
        "bound_design": debug._proof(design), "source_families": families,
        "implementation_sha256": debug.implementation_sha256()})
    declaration, _ = debug._load_declaration(path)
    declaration["_self_proof"] = debug._proof(path)
    return path, declaration


def selection_decision(tmp_path, declaration, selected="capacity300"):
    package = tmp_path/"selection_models"
    child = package/"synthetic.txt"
    child.parent.mkdir();child.write_text("synthetic only")
    manifest = write(package/"manifest.json", {"kind": debug.PACKAGE_KIND, "phase": "selection",
        "native_fit_count": 3, "execution_years": [2023], "models": list(debug.CANDIDATES),
        "completed_at": "2026-10-21T11:20:00+00:00",
        "inputs": {"declaration": declaration["_self_proof"]}, "files": {child.name: debug.primary._sha(child)}})
    report = write(tmp_path/"selection_report.json", {"phase": "selection", "model_manifest": manifest,
        "selection": {"selected_candidate": selected}})
    decision_path = tmp_path/"decision.json"
    write(decision_path, {"kind": "a14_model_debug_selection_decision", "selected_candidate": selected,
        "declared_at": "2026-10-21T11:21:00+00:00", "selection_report": report,
        "selection_model_manifest": manifest})
    return decision_path


def rewrite_decision_file(path, update, *, proof_key=None):
    value = json.loads(path.read_text())
    if proof_key:
        child = Path(value[proof_key]["path"])
        payload = json.loads(child.read_text());update(payload)
        value[proof_key] = write(child, payload)
    else:
        update(value)
    write(path, value)


def panel():
    rng = np.random.default_rng(20261021)
    calendar = pd.bdate_range("2020-01-01", periods=80)
    index = pd.MultiIndex.from_product([calendar[:35], [f"SH{600000+i}" for i in range(60)]],
        names=["datetime", "instrument"])
    features = pd.DataFrame(rng.normal(size=(len(index), 158)).astype(np.float32),
        index=index, columns=[f"f{i}" for i in range(158)])
    label = (features.f0+features.f1*.2).rename("LABEL0")
    frame = pd.concat({"feature": features, "label": label.to_frame()}, axis=1)
    fold = {"train_start": str(calendar[0].date()), "train_signal_end": str(calendar[34].date()),
        "data_cutoff": str(calendar[40].date()), "execution_year": 2021}
    return frame, fold, calendar


def test_fixed_candidate_capacity_and_window_are_exact_and_no_seed_search():
    assert list(debug.CANDIDATES) == ["capacity300", "regularized300", "recent3_capacity300"]
    baseline = asdict(FixedQlibLGBSpec())
    assert debug.CANDIDATES["capacity300"] == {"lookback_calendar_years": 5,
        "spec": {**baseline, "num_boost_round": 300}}
    assert debug.CANDIDATES["regularized300"]["spec"] == {**baseline, "num_boost_round": 300,
        "num_leaves": 15, "max_depth": 4, "min_data_in_leaf": 500, "lambda_l2": 10.0}
    assert debug.CANDIDATES["recent3_capacity300"]["spec"] == debug.CANDIDATES["capacity300"]["spec"]
    assert debug.CANDIDATES["recent3_capacity300"]["lookback_calendar_years"] == 3


def test_complete_frozen_design_is_checked_not_just_candidate_ids(tmp_path):
    path, _ = bound_declaration(tmp_path)
    declaration = json.loads(path.read_text())
    design = json.loads(Path(declaration["bound_design"]["path"]).read_text())
    design["confirmation"]["fit_only_selected_candidate"] = False
    declaration["bound_design"] = write(tmp_path/"changed_design.json", design)
    write(path, declaration)
    with pytest.raises(ValueError, match="complete pre-result"):
        debug._load_declaration(path)


@pytest.mark.parametrize("change", ["implementation", "candidate", "source_family", "source_byte"])
def test_declaration_rejects_tampered_mechanics_and_proofs(tmp_path, change):
    path, _ = bound_declaration(tmp_path);declaration=json.loads(path.read_text())
    if change == "implementation": declaration["implementation_sha256"]["native_trainer"] = "bad"
    elif change == "candidate": declaration["candidates"] = {"other": {}}
    elif change == "source_family": declaration["source_families"].pop("primary")
    else: Path(declaration["source_families"]["primary"]["protocol"]["path"]).write_text("changed")
    write(path, declaration)
    with pytest.raises(ValueError): debug._load_declaration(path)


def test_selection_phase_has_exact_three_fits_2023_only_and_no_decision(tmp_path):
    _, declaration = bound_declaration(tmp_path)
    plan = debug.phase_plan(declaration, phase="selection")
    assert plan["years"] == [2023] and plan["native_fit_count"] == 3
    assert plan["candidates"] == list(debug.CANDIDATES)
    with pytest.raises(ValueError, match="later candidate"):
        debug.phase_plan(declaration, phase="selection", selection_path="later")


@pytest.mark.parametrize("phase,years,count", [("confirmation", [2024], 1), ("retrospective", [2025, 2026], 2)])
def test_later_phase_requires_same_bound_prior_selection_and_only_one_candidate(tmp_path, phase, years, count):
    _, declaration = bound_declaration(tmp_path)
    with pytest.raises(ValueError, match="same prior selection"):
        debug.phase_plan(declaration, phase=phase)
    decision = selection_decision(tmp_path, declaration, "regularized300")
    plan = debug.phase_plan(declaration, phase=phase, selection_path=decision)
    assert plan["candidates"] == ["regularized300"] and plan["years"] == years
    assert plan["native_fit_count"] == count and plan["selection_decision"] == debug._proof(decision)


@pytest.mark.parametrize("mutation", ["different_candidate", "unknown_candidate", "decision_before_fit", "naive_time",
    "model_bytes", "wrong_model_phase", "wrong_year", "extra_fit", "wrong_protocol", "wrong_report_phase", "wrong_report_model"])
def test_later_phase_rejects_unbound_or_later_selection_evidence(tmp_path, mutation):
    _, declaration = bound_declaration(tmp_path);decision=selection_decision(tmp_path, declaration)
    if mutation == "different_candidate":
        rewrite_decision_file(decision, lambda d:d.update(selected_candidate="regularized300"))
    elif mutation == "unknown_candidate":
        rewrite_decision_file(decision, lambda d:d.update(selected_candidate="new_grid"))
    elif mutation == "decision_before_fit":
        rewrite_decision_file(decision, lambda d:d.update(declared_at="2026-10-21T11:19:59+00:00"))
    elif mutation == "naive_time":
        rewrite_decision_file(decision, lambda d:d.update(declared_at="2026-10-21T11:21:00"))
    elif mutation == "model_bytes": (tmp_path/"selection_models/synthetic.txt").write_text("changed")
    elif mutation == "wrong_model_phase":
        rewrite_decision_file(decision, lambda d:d.update(phase="confirmation"), proof_key="selection_model_manifest")
    elif mutation == "wrong_year":
        rewrite_decision_file(decision, lambda d:d.update(execution_years=[2024]), proof_key="selection_model_manifest")
    elif mutation == "extra_fit":
        rewrite_decision_file(decision, lambda d:d.update(native_fit_count=4), proof_key="selection_model_manifest")
    elif mutation == "wrong_protocol":
        rewrite_decision_file(decision, lambda d:d.update(inputs={"declaration":{"path":"other","sha256":"bad"}}), proof_key="selection_model_manifest")
    elif mutation == "wrong_report_phase":
        rewrite_decision_file(decision, lambda d:d.update(phase="retrospective"), proof_key="selection_report")
    else:
        rewrite_decision_file(decision, lambda d:d.update(model_manifest={"path":"other","sha256":"bad"}), proof_key="selection_report")
    with pytest.raises(ValueError): debug.phase_plan(declaration, phase="confirmation", selection_path=decision)


def test_recent_window_changes_start_only_preserving_actual_maturity_and_cutoff():
    fold = {"execution_year": 2023, "train_start": "2018-01-01", "train_signal_end": "2022-12-22",
        "data_cutoff": "2022-12-30", "first_execution_session": "2023-01-03"}
    recent = debug.candidate_fold(fold, "recent3_capacity300")
    assert recent == {**fold, "train_start": "2020-01-01"}
    assert debug.candidate_fold(fold, "capacity300") == fold
    with pytest.raises(ValueError, match="Unknown"): debug.candidate_fold(fold, "other")


def test_train_only_processors_and_target_rank_are_saved_and_future_rows_rejected():
    pytest.importorskip("qlib")
    frame, fold, calendar = panel()
    learned, processors = debug.prepare_candidate_panel(frame, fold, calendar)
    np.testing.assert_array_equal(processors["processors"][0]["mean_train"], frame["feature"].median().to_numpy())
    expected = (frame["label"].groupby("datetime").rank(pct=True)-.5)*3.46
    pd.testing.assert_frame_equal(learned["label"], expected, check_dtype=False)
    later = frame.iloc[:60].copy()*100000
    later.index = pd.MultiIndex.from_product([calendar[[35]], frame.index.get_level_values("instrument").unique()],
        names=frame.index.names)
    with pytest.raises(ValueError, match="Immature"):
        debug.prepare_candidate_panel(pd.concat([frame, later]), fold, calendar)
    again, saved_again = debug.prepare_candidate_panel(frame, fold, calendar)
    assert processors == saved_again
    pd.testing.assert_frame_equal(learned, again)


@pytest.mark.parametrize("part", ["feature", "label", "index", "schema", "nonfinite"])
def test_actual_consumed_view_rejects_feature_label_index_or_scope_tampering(part):
    frame, _, _ = panel()
    view = debug.ConsumedPanel(frame, debug.feature_digest(frame["feature"]), debug._label_digest(frame["label"].iloc[:,0]))
    if part == "feature": frame.iloc[0,0] += 1
    elif part == "label": frame.iloc[0,-1] += 1
    elif part == "index": frame.index=frame.index[::-1]
    elif part == "schema":
        with pytest.raises(ValueError, match="single training"):
            view.prepare("valid", col_set=["feature","label"], data_key="learn")
        return
    else: frame.iloc[0,-1] = np.nan
    with pytest.raises(ValueError): view.prepare("train", col_set=["feature","label"], data_key="learn")


def test_native_candidate_exact300_trees_consumes_declared_panel_and_roundtrips(tmp_path):
    pytest.importorskip("qlib");pytest.importorskip("lightgbm")
    frame, fold, calendar = panel()
    learned, processors = debug.prepare_candidate_panel(frame, fold, calendar)
    feature_sha = debug.feature_digest(learned["feature"])
    label_sha = debug._label_digest(learned["label"].iloc[:,0])
    view=debug.ConsumedPanel(learned, feature_sha, label_sha)
    spec=FixedQlibLGBSpec(**debug.CANDIDATES["capacity300"]["spec"])
    model=fit_fixed_qlib_lightgbm(view,spec=spec)
    assert model.booster.num_trees()==model.booster.current_iteration()==300
    assert view.consume_count==1 and view.consumed_features==feature_sha and view.consumed_labels==label_sha
    model.save_native(tmp_path/"model",metadata={"fitted_inference_processors":processors["processors"]})
    loaded=load_fixed_qlib_model(tmp_path/"model")
    assert loaded.spec==spec and loaded.training_rows==len(learned)
    np.testing.assert_array_equal(model.booster.predict(learned["feature"],num_threads=1),
        loaded.booster.predict(learned["feature"],num_threads=1))


def test_rank_diagnostics_known_counts_ties_and_optimism_are_explicit():
    frame, _, _=panel();labels=frame["label"].iloc[:,0]
    result=debug.score_diagnostics(labels*7,labels,role="training_in_sample")
    assert result["mean_daily_rank_ic"]==1 and result["known_rank_ic_days"]==35
    assert result["in_sample_optimism"] is True and result["independent_observations_claim"] is False
    assert debug.score_diagnostics(-labels,labels,role="confirmation")["mean_daily_rank_ic"]==-1
    sparse=labels.iloc[:19]
    assert debug.score_diagnostics(sparse,sparse,role="confirmation")["known_rank_ic_days"]==0
    changed=labels.copy();changed.iloc[0]=np.nan
    assert debug.score_diagnostics(labels,changed,role="confirmation")["excluded_nonfinite_rows"]==1
    with pytest.raises(ValueError,match="alignment"):
        debug.score_diagnostics(labels.iloc[::-1],labels,role="confirmation")


def test_original_raw_h5_open_endpoints_are_verified_without_future_membership(monkeypatch):
    calendar=pd.bdate_range("2020-01-01",periods=20)
    index=pd.MultiIndex.from_product([calendar[:4],["SH600000"]],names=["datetime","instrument"])
    prices=(10+np.arange(20)*.2).astype(np.float32)
    labels=prices[6:10]/prices[1:5]-1
    frame=pd.concat({"label":pd.DataFrame({"LABEL0":labels},index=index)},axis=1)
    monkeypatch.setattr(debug,"read_qlib_field",lambda *args:prices)
    debug._verify_raw_labels(frame,SimpleNamespace(dataset_dir=Path("/synthetic")),calendar)
    frame.iloc[0,0]+=.01
    with pytest.raises(ValueError,match="next-open"):
        debug._verify_raw_labels(frame,SimpleNamespace(dataset_dir=Path("/synthetic")),calendar)


def fake_pipeline(monkeypatch, tmp_path, declaration):
    calendar=pd.bdate_range("2017-01-02","2026-10-19")
    intervals={f"SH{600000+i}":[(calendar[0],calendar[-1])] for i in range(25)}
    folds={}
    for year in (2023,2024,2025,2026):
        prior=calendar[calendar < f"{year}-01-01"];current=calendar[calendar.year==year]
        folds[year]={"execution_year":year,"train_start":f"{year-5}-01-01", "data_cutoff":str(prior[-1].date()),
            "train_signal_end":str(prior[-7].date()),"signal_start":str(prior[-1].date()),
            "signal_end":str(current[-1].date()),"first_execution_session":str(current[0].date())}
    class Eligibility:
        def mask(self,index,intervals,**kwargs):return np.ones(len(index),bool),{"synthetic":True}
    source=SimpleNamespace(path=tmp_path/"synthetic_source",dataset_dir=tmp_path,manifest_sha256="synthetic")
    scope=SimpleNamespace(assert_unchanged=lambda:None)
    monkeypatch.setattr(debug,"_source_context",lambda family,**kwargs:{"source":source,"calendar":calendar,
        "scope":scope,"intervals":intervals,"eligibility":Eligibility(),"source_files":{},
        "membership_audit":{"status":"passed"},"folds":folds,"audit":{},"stock_basic":"synthetic",
        "package":tmp_path,"family":family,"scope_manifest_sha256":"synthetic"})
    monkeypatch.setattr(debug.primary,"assert_market_originals_unchanged",lambda *args:None)
    monkeypatch.setattr(debug,"_copy_baseline",lambda context,year,target:{"execution_year":year,"native_refit":False})
    monkeypatch.setattr(debug,"_verify_raw_labels",lambda *args:None)
    def raw(source,calendar,intervals,start,end):
        dates=calendar[(calendar>=start)&(calendar<=end)][::120]
        index=pd.MultiIndex.from_product([dates,intervals],names=["datetime","instrument"])
        values=np.arange(len(index),dtype=np.float32)
        features=pd.DataFrame({f"f{i}":values+i for i in range(158)},index=index)
        return pd.concat({"feature":features,"label":pd.DataFrame({"LABEL0":values},index=index)},axis=1)
    monkeypatch.setattr(debug.primary,"_raw_alpha158",raw)
    def prepare(frame,fold,calendar):
        debug.validate_training_rows(frame,fold,calendar)
        learned=frame.copy();learned["label"]=(frame["label"].groupby("datetime").rank(pct=True)-.5)*3.46
        return learned,{"processors":[],"feature_names":list(frame["feature"].columns)}
    monkeypatch.setattr(debug,"prepare_candidate_panel",prepare)
    calls=[]
    class Booster:
        def predict(self,features,**kwargs):return features.iloc[:,0].to_numpy(dtype=float)
        def current_iteration(self):return 300
        def num_trees(self):return 300
        def feature_importance(self,**kwargs):return np.ones(158)
    class Model:
        def __init__(self,view,spec):
            learned=view.prepare("train",col_set=["feature","label"],data_key="learn")
            self.feature_names=list(learned["feature"].columns);self.booster=Booster()
            self.spec=spec;self.training_rows=len(learned)
        def save_native(self,path,**kwargs):
            path.mkdir();(path/"model.txt").write_text("synthetic model only")
    def fit(view,*,spec):
        calls.append((str(view.learned.index.get_level_values("datetime").max().date()),asdict(spec)))
        return Model(view,spec)
    monkeypatch.setattr(debug,"fit_fixed_qlib_lightgbm",fit)
    monkeypatch.setattr(debug,"apply_frozen_processors",lambda features,model,processors:
        pd.Series(model.booster.predict(features),index=features.index))
    return calls


@pytest.mark.parametrize("phase,expected_years,expected_count",[("selection",[2023],3),
    ("confirmation",[2024],1),("retrospective",[2025,2026],2)])
def test_synthetic_pipeline_respects_exact_phase_budget_and_keeps_current_paper_untouched(
        tmp_path,monkeypatch,phase,expected_years,expected_count):
    path,declaration=bound_declaration(tmp_path)
    decision=selection_decision(tmp_path,declaration) if phase!="selection" else None
    calls=fake_pipeline(monkeypatch,tmp_path,declaration)
    active=tmp_path/"paper";active.mkdir();sentinel=active/"state.json";sentinel.write_text("preserve")
    value=json.loads(path.read_text());value["active_paper_directory"]=str(active)
    # The declared path proof must be updated in this synthetic staged input.
    write(path,value);declaration["_self_proof"]=debug._proof(path)
    if decision:
        package=tmp_path/"selection_models/manifest.json";manifest=json.loads(package.read_text())
        manifest["inputs"]["declaration"]=debug._proof(path);proof=write(package,manifest)
        report=tmp_path/"selection_report.json";r=json.loads(report.read_text());r["model_manifest"]=proof;report_proof=write(report,r)
        d=json.loads(decision.read_text());d.update(selection_model_manifest=proof,selection_report=report_proof);write(decision,d)
    output=tmp_path/f"models_{phase}"
    result=debug.build_debug_predictions(path,output,phase=phase,selection_path=decision)
    manifest=result["manifest"]
    assert len(calls)==manifest["native_fit_count"]==expected_count
    assert manifest["execution_years"]==expected_years
    assert {record["execution_year"] for record in manifest["folds"]}==set(expected_years)
    assert len(set(result["predictions"].model))==(3 if phase=="selection" else 1)
    assert sentinel.read_text()=="preserve" and manifest["active_paper_changed"] is False
    assert manifest["new_future_out_of_sample_claim"] is False
    with pytest.raises(ValueError):debug.build_debug_predictions(path,output,phase=phase,selection_path=decision)


def test_synthetic_midrun_source_change_preserves_failure_artifact_and_blocks_completed_package(tmp_path,monkeypatch):
    path,declaration=bound_declaration(tmp_path);fake_pipeline(monkeypatch,tmp_path,declaration)
    changed=False
    def progress(event):
        nonlocal changed
        if event["phase"]=="candidate_complete" and not changed:
            Path(declaration["source_families"]["primary"]["protocol"]["path"]).write_text("changed")
            changed=True
    output=tmp_path/"failed_generation"
    with pytest.raises(ValueError,match="bound model-debug input changed"):
        debug.build_debug_predictions(path,output,phase="selection",progress=progress)
    assert (output/"failure.json").is_file() and not (output/"manifest.json").exists()


def test_protected_active_paper_target_is_rejected_before_loading_sources(tmp_path,monkeypatch):
    path,_=bound_declaration(tmp_path)
    active=tmp_path/"paper";value=json.loads(path.read_text());value["active_paper_directory"]=str(active);write(path,value)
    monkeypatch.setattr(debug,"_source_context",lambda *args,**kwargs:pytest.fail("must reject before loading"))
    with pytest.raises(ValueError,match="current 20-stock"):
        debug.build_debug_predictions(path,active/"models",phase="selection")
