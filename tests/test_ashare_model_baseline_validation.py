"""Synthetic A16 controls and an exact AST guard on the original execution."""
import ast
from copy import deepcopy
import hashlib
import inspect

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_model_baseline_validation as baseline
from stopat30m import ashare_model_debug_validation as debug
from stopat30m import ashare_model_debug_research as training
from stopat30m import ashare_turnover_validation as turnover
from test_ashare_model_debug_validation import context, fake_package, bind_verifier


def source(tmp_path):
    value = context(tmp_path)
    instruments = value["scores"]["LGB"].index.get_level_values("instrument").unique()
    value["normalized"] = pd.DataFrame({inst: np.arange(len(value["calendar"]), dtype=float)+100+number
        for number, inst in enumerate(instruments)}, index=value["calendar"])
    return value


def test_static_execution_clone_changes_only_name_and_rank_call():
    original = ast.parse(inspect.getsource(turnover.simulate_turnover)).body[0]
    cloned = ast.parse(inspect.getsource(baseline._simulate_simple_turnover)).body[0]
    cloned.name = original.name
    changed = 0
    for node in ast.walk(cloned):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_baseline_rank":
            node.func.id = "_rank"
            changed += 1
    assert changed == 1
    assert ast.dump(cloned, include_attributes=False) == ast.dump(original, include_attributes=False)
    assert baseline._engine is turnover._engine
    assert baseline.HoldingAgeTracker is turnover.HoldingAgeTracker
    assert baseline.daily_inputs is turnover.daily_inputs
    assert baseline.path_stability is turnover.path_stability


def test_original_entire_result_exact_and_input_unchanged(tmp_path):
    value = source(tmp_path)
    scores = value["scores"]["LGB"].copy()
    expected = debug.simulate_debug_model(value, candidate="baseline", period="2025")
    actual = baseline.simulate_baseline_strategy(value, strategy="original", period="2025")
    assert actual == expected
    assert "baseline_control" not in value
    pd.testing.assert_series_equal(value["scores"]["LGB"], scores)


def test_regularized_reuses_verified_scores_and_unchanged_real_engine(tmp_path, monkeypatch):
    value = source(tmp_path)
    path, manifest = fake_package(tmp_path, value, candidate="regularized300")
    # The tiny synthetic original fixture has only 2025 signals. Restrict the
    # declared retrospective years for this test of the real execution path.
    monkeypatch.setitem(training.PHASE_YEARS, "retrospective", [2025])
    manifest["execution_years"] = [2025]
    bind_verifier(monkeypatch, {path: manifest})
    expected = debug.simulate_debug_model(value, candidate="regularized300", period="2025", prediction_package=path)
    actual = baseline.simulate_baseline_strategy(value, strategy="regularized300", period="2025", prediction_package=path)
    assert actual == expected


@pytest.mark.parametrize("strategy,lookback,sign", [("MOM60", 60, 1), ("REV20", 20, -1)])
def test_exact_adjusted_price_endpoints_full_index_and_no_labels(tmp_path, strategy, lookback, sign):
    value = source(tmp_path)
    value["labels"] = pd.Series(np.nan, index=value["scores"]["LGB"].index)
    scores = baseline.baseline_signal_scores(value, strategy, period="2025")
    assert scores.index.equals(value["scores"]["LGB"].index)
    for (date, inst), score in scores.iloc[::101].items():
        day = date.date().isoformat()
        position = value["calendar"].index(day)
        expected = sign*(value["normalized"].at[day, inst] /
                         value["normalized"].at[value["calendar"][position-lookback], inst]-1)
        assert score == expected
    assert scores.notna().all()
    value["labels"][:] = 999
    pd.testing.assert_series_equal(scores, baseline.baseline_signal_scores(value, strategy, period="2025"))


@pytest.mark.parametrize("seed", baseline.HASH_SEEDS)
def test_hash_is_declared_static_security_seed_not_date_or_label(tmp_path, seed):
    value = source(tmp_path)
    strategy = "HASH"+str(seed)
    scores = baseline.baseline_signal_scores(value, strategy, period="2025")
    for inst in scores.index.get_level_values("instrument").unique():
        expected = int(hashlib.sha256(f"a16-static-hash-v1|{seed}|{inst}".encode()).hexdigest()[:13],16)/2**52
        assert scores.xs(inst, level="instrument").eq(expected).all()
    assert scores.index.equals(value["scores"]["LGB"].index)
    assert scores.ge(0).all() and scores.lt(1).all()


@pytest.mark.parametrize("strategy,lookback", [("MOM60",60),("REV20",20)])
@pytest.mark.parametrize("fault", ["nan", "inf", "zero", "negative", "missing_column", "before_history"])
def test_real_absent_endpoints_stay_nan_excluded_and_disclosed(tmp_path, strategy, lookback, fault):
    value = source(tmp_path)
    date, inst = value["scores"]["LGB"].index[0]
    day = date.date().isoformat()
    previous = value["calendar"][value["calendar"].index(day)-lookback]
    if fault == "missing_column": value["normalized"] = value["normalized"].drop(columns=inst)
    elif fault == "before_history":
        # Keep all raw inference dates but genuinely truncate the available
        # actual calendar, rather than inventing an endpoint observation.
        value["calendar"] = value["calendar"][value["calendar"].index(day):]
        value["normalized"] = value["normalized"].loc[value["calendar"]]
    else: value["normalized"].at[previous, inst] = {"nan":np.nan,"inf":np.inf,"zero":0,"negative":-1}[fault]
    prepared = baseline.prepare_baseline_context(value, strategy=strategy, period="2025")
    assert pd.isna(prepared["scores"]["LGB"].loc[(date,inst)])
    ranked, _, _ = baseline._baseline_rank(prepared, "LGB", day)
    assert inst not in ranked
    coverage = prepared["score_coverage"][("LGB",day)]
    assert inst in coverage["missing_required_scores"]
    assert {"instrument":inst,"reason":f"unobserved_declared_{lookback}_session_endpoint"} in coverage["explicit_exceptions"]
    assert prepared["baseline_control"]["missing_endpoint_rows_by_signal_date"][day] >= 1


@pytest.mark.parametrize("strategy", ["MOM60","REV20","HASH1103"])
@pytest.mark.parametrize("damage", ["nan", "missing_identity"])
def test_observed_endpoint_or_hash_score_damage_raises(tmp_path, strategy, damage):
    value = source(tmp_path)
    prepared = baseline.prepare_baseline_context(value, strategy=strategy, period="2025")
    key = prepared["scores"]["LGB"].index[0]
    if damage == "nan": prepared["scores"]["LGB"].loc[key] = np.nan
    else: prepared["scores"]["LGB"] = prepared["scores"]["LGB"].drop(key)
    with pytest.raises(ValueError,match="Missing control score"):
        baseline._baseline_rank(prepared,"LGB",key[0].date().isoformat())


def test_missing_inference_identity_cannot_be_endpoint_exception(tmp_path):
    value = source(tmp_path)
    key = value["scores"]["LGB"].index[0]
    value["normalized"] = value["normalized"].drop(columns=key[1])
    prepared = baseline.prepare_baseline_context(value,strategy="MOM60",period="2025")
    prepared["scores"]["LGB"] = prepared["scores"]["LGB"].drop(key)
    with pytest.raises(ValueError,match="Missing control score"):
        baseline._baseline_rank(prepared,"LGB",key[0].date().isoformat())


def test_same_finite_simple_scores_keep_every_execution_record_except_coverage_annotation(tmp_path):
    value = source(tmp_path)
    prepared = baseline.prepare_baseline_context(value,strategy="HASH1103",period="2025")
    expected = turnover.simulate_turnover(deepcopy(prepared),period="2025",variant=dict(debug.HOLD_RULE))
    actual = baseline.simulate_baseline_strategy(value,strategy="HASH1103",period="2025")
    for coverage in actual["records"]["score_coverage"]:
        assert coverage.pop("strategy") == "HASH1103"
    assert actual == expected


def test_membership_delisting_actions_and_partial_quotes_keep_original_rules(tmp_path):
    value = source(tmp_path)
    day = value["scores"]["LGB"].index[0][0].date().isoformat()
    instruments = list(value["scores"]["LGB"].index.get_level_values("instrument").unique())
    value["pool"].members_on = lambda day: instruments[:-1]
    value["actions"].excluded = lambda inst, *a, **k: inst == instruments[0]
    prepared = baseline.prepare_baseline_context(value,strategy="HASH1103",period="2025")
    ranked,members,_ = baseline._baseline_rank(prepared,"LGB",day)
    assert instruments[0] not in ranked and instruments[-1] not in ranked
    assert members == instruments[:-1]
    old = deepcopy(prepared)
    old["score_coverage"] = {}
    assert turnover._rank(old,"LGB",day) == (ranked,members,value["raw"].loc[value["raw"].date.eq(day)].set_index("instrument").close.to_dict())


@pytest.mark.parametrize("fault", ["unknown_strategy","wrong_period","simple_package","original_package",
    "changed_declaration","duplicate_index","bad_index_names","intraday","bad_calendar","misaligned_prices"])
def test_invalid_contracts_rejected_before_execution(tmp_path,fault):
    value = source(tmp_path)
    strategy,period,package = "MOM60","2025",None
    if fault == "unknown_strategy": strategy="HASH9999"
    elif fault == "wrong_period": period="2023"
    elif fault in {"simple_package","original_package"}:
        package="unauthorized"
        if fault=="original_package": strategy="original"
    elif fault == "changed_declaration":
        from pathlib import Path
        Path(value["model_debug"]["training_declaration"]["path"]).write_text("changed")
    elif fault=="duplicate_index": value["scores"]["LGB"]=pd.concat([value["scores"]["LGB"],value["scores"]["LGB"].iloc[:1]])
    elif fault=="bad_index_names": value["scores"]["LGB"].index=value["scores"]["LGB"].index.set_names(["date","stock"])
    elif fault=="intraday":
        old=value["scores"]["LGB"].index
        value["scores"]["LGB"].index=pd.MultiIndex.from_arrays([old.get_level_values(0)+pd.Timedelta(hours=1),old.get_level_values(1)],names=old.names)
    elif fault=="bad_calendar": value["calendar"]=[*value["calendar"],value["calendar"][-1]]
    elif fault=="misaligned_prices": value["normalized"]=value["normalized"].iloc[1:]
    with pytest.raises((ValueError,TypeError)):
        baseline.prepare_baseline_context(value,strategy=strategy,period=period,prediction_package=package)


def test_diagnostic_api_requires_explicit_prepared_contract_and_returns_copy(tmp_path):
    value = source(tmp_path)
    with pytest.raises(ValueError): baseline.baseline_signal_scores(value)
    with pytest.raises(ValueError): baseline.baseline_signal_scores(value,"MOM60")
    prepared=baseline.prepare_baseline_context(value,strategy="REV20",period="2025")
    scores=baseline.baseline_signal_scores(prepared)
    scores.iloc[0]=999
    assert prepared["scores"]["LGB"].iloc[0]!=999
