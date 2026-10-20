"""The matched target trial cannot run new returns before full old replay."""
from contextlib import nullcontext
from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_horizon_experiment as experiment
from stopat30m import ashare_horizon_validation as validation
from stopat30m.ashare_horizon_protocol import MODELS, PERIODS, TRAINING, EXECUTION, _assert_design


def paths():
    return [{"arm": arm, "period": period, "performance_valid": True,
        "metrics": {"total_return_lower_exact": "0.05", "max_drawdown_conservative_exact": "-0.15",
                    "one_way_turnover_average_nav": 8.0}}
        for arm in MODELS for period in PERIODS]


def improve(rows):
    for row in rows:
        if row["arm"] == "H20":
            row["metrics"].update(total_return_lower_exact="0.08",
                max_drawdown_conservative_exact="-0.12", one_way_turnover_average_nav=7.0)


def test_equal_models_do_not_establish_horizon_improvement():
    result = experiment.decide_horizon_comparison(paths())
    assert not result["H20_improves_under_declared_historical_comparison"]
    assert result["model_selected_for_active_paper"] is None


def test_predeclared_profit_and_risk_gate_all_pass_still_does_not_activate():
    rows = paths(); improve(rows)
    result = experiment.decide_horizon_comparison(rows)
    assert result["H20_improves_under_declared_historical_comparison"]
    assert result["next_research"] == "H20_candidate_requires_new_forward_evidence"
    assert result["model_selected_for_active_paper"] is None
    assert result["new_future_out_of_sample_claim"] is False


@pytest.mark.parametrize("fault", ["invalid_control", "invalid_candidate", "losing_year", "deeper_drawdown", "higher_turnover", "no_return_gain"])
def test_each_gate_can_prevent_h20_qualification(fault):
    rows = paths(); improve(rows)
    h20 = next(row for row in rows if row["arm"] == "H20" and row["period"] == "continuous")
    if fault == "invalid_control":
        rows[0]["performance_valid"] = False
    elif fault == "invalid_candidate":
        h20["performance_valid"] = False
    elif fault == "losing_year":
        next(row for row in rows if row["arm"] == "H20" and row["period"] == "2026")["metrics"]["total_return_lower_exact"] = "-0.01"
    elif fault == "deeper_drawdown":
        h20["metrics"]["max_drawdown_conservative_exact"] = "-0.16"
    elif fault == "higher_turnover":
        h20["metrics"]["one_way_turnover_average_nav"] = 8.1
    else:
        h20["metrics"]["total_return_lower_exact"] = "0.05"
    result = experiment.decide_horizon_comparison(rows)
    assert not result["H20_improves_under_declared_historical_comparison"]
    assert result["model_selected_for_active_paper"] is None
    if fault.startswith("invalid"):
        assert result["next_research"] == "resolve_invalid_paths_no_model_selection"


@pytest.mark.parametrize("fault", ["missing", "duplicate", "extra"])
def test_all_six_accounts_required_exactly_once(fault):
    rows = paths()
    if fault == "missing":
        rows.pop()
    elif fault == "duplicate":
        rows.append(rows[0])
    else:
        rows[-1]["arm"] = "H10"
    with pytest.raises(ValueError, match="six"):
        experiment.decide_horizon_comparison(rows)


@pytest.mark.parametrize("field", ["training", "execution", "fixed_fit_budget", "fixed_ledger_budget", "minimum_evidence_for_H20_improvement"])
def test_design_cannot_expand_or_weaken_after_results(field):
    from pathlib import Path
    design = json.loads((Path(__file__).parents[1]/"docs/research/a8_shared_panel_horizon_design_v1.json").read_text())
    _assert_design(design)
    altered = deepcopy(design)
    if field == "training":
        altered[field]["common_purge_future_sessions"] = 6
    elif field == "execution":
        altered[field]["rebalance_sessions"] = 5
    elif field == "minimum_evidence_for_H20_improvement":
        altered[field]["all_six_paths_accounting_valid"] = False
    else:
        altered[field] += 1
    with pytest.raises(ValueError):
        _assert_design(altered)
    assert TRAINING["common_endpoint_offsets"] == [1, 6, 21]
    assert EXECUTION["rebalance_sessions"] == 20


@pytest.mark.parametrize("bad_period", ["2025", "2026", "continuous"])
def test_any_baseline_mismatch_stops_every_new_return(tmp_path, monkeypatch, bad_period):
    source = tmp_path/"synthetic_source.py"; source.write_text("# synthetic non-executed source\n")
    proof = {"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
    bundle = tmp_path/"bundle.json"; bundle.write_text(json.dumps({"snapshot_path": "snapshot", "scope_path": "scope"}))
    models = tmp_path/"models"; models.mkdir(); (models/"predictions.csv").write_text("model,score\nH5,0.1\nH20,0.2\n")
    bound = {key: proof for key in ["synthetic_source", "training_protocol", "original_protocol",
        "market_audit", "original_model_manifest", "merger_actions"]}
    bound["source_bundle"] = {"path": str(bundle), "sha256": hashlib.sha256(bundle.read_bytes()).hexdigest()}
    declaration = {"kind": "a8_shared_panel_horizon_execution_declaration", "execution": EXECUTION,
        "models": list(MODELS), "periods": PERIODS, "all_four_models_verified_before_any_new_returns": True,
        "paper_or_broker_change_allowed": False, "bound_inputs": bound,
        "models_directory": str(models), "baseline_directory": str(tmp_path/"old_baseline"), "decision_gate": {"synthetic": True}}
    path = tmp_path/"declaration.json"; path.write_text(json.dumps(declaration))
    monkeypatch.setattr(experiment.stability, "_assert_proofs", lambda _: None)
    monkeypatch.setattr(experiment, "execution_proofs", lambda: {"synthetic_source": proof})
    monkeypatch.setattr(experiment, "load_horizon_protocol", lambda _: {"design": {"minimum_evidence_for_H20_improvement": {"synthetic": True}}})
    monkeypatch.setattr(experiment, "_verify_models", lambda *args: {})
    monkeypatch.setattr(experiment, "_baseline_proofs", lambda _: {})
    context = {"pool": SimpleNamespace(verified_batch=nullcontext), "calendar": [], "bound": {}}
    monkeypatch.setattr(experiment.stability, "load_stability_context", lambda *args: context)
    index = pd.MultiIndex.from_tuples([(pd.Timestamp("2025-01-02"), "SH600000")], names=["datetime", "instrument"])
    monkeypatch.setattr(validation, "horizon_execution_year_scores", lambda *args, **kwargs: pd.Series([.1], index=index))
    monkeypatch.setattr(experiment.stability, "simulate_stability", lambda *args, period, **kwargs: {"period": period})
    monkeypatch.setattr(experiment.stability, "score_quality", lambda *args, **kwargs: {})
    called = []
    def baseline(result, _):
        called.append(result["period"])
        if result["period"] == bad_period:
            raise ValueError("synthetic baseline mismatch")
        return {"passed": True}
    monkeypatch.setattr(experiment, "assert_full_baseline_reproduced", baseline)
    monkeypatch.setattr(validation, "simulate_horizon", lambda *args, **kwargs: pytest.fail("new returns ran before all baseline gates"))
    output = tmp_path/"output"
    with pytest.raises(ValueError, match="baseline mismatch"):
        experiment.run_horizon_comparison(path, output)
    failure = json.loads((output/"failure.json").read_text())
    assert failure["completed_new_paths"] == 0
    assert len(called) == list(PERIODS).index(bad_period)+1
