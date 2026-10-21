"""Synthetic A11 orchestration gates; no historical simulator, fit or network."""
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json
import sys

import pytest

from stopat30m import ashare_capital_turnover_experiment as experiment
from test_ashare_position_count_experiment import _reference, _write


def _design():
    return json.loads((Path(__file__).parents[1] / "docs/research/a11_capital_turnover_design_v1.json").read_bytes())


def _metrics():
    return {"total_return_lower_exact": "0.10", "max_drawdown_conservative_exact": "-0.20",
        "total_fee_cny": "1000.00", "slippage_cost_cny": "100.00",
        "one_way_turnover_average_nav": 10.0, "mean_invested_fraction": .9,
        "positive_month_fraction": .5, "rolling_63_sessions": {"minimum": -.1}}


def _risk():
    return {"coverage_complete": True, "max_single_nav_fraction": .10,
        "max_top3_nav_fraction": .30, "min_effective_stock_count": 10.0}


def _paths():
    return [{"arm_key": key, "fraction": fraction, "period": period,
        "performance_valid": True, "metrics": _metrics(), "diagnostics": {"risk": _risk()}}
        for key, fraction in experiment.CAPITAL_ARMS for period in experiment.PERIODS]


def _row(rows, key="cap5pct", period="continuous"):
    return next(r for r in rows if r["arm_key"] == key and r["period"] == period)


def _qualify(rows, key="cap5pct"):
    for period in experiment.PERIODS:
        _row(rows, key, period)["metrics"].update(total_return_lower_exact="0.20",
            max_drawdown_conservative_exact="-0.15", total_fee_cny="800.00",
            slippage_cost_cny="50.00", one_way_turnover_average_nav=8.0,
            mean_invested_fraction=.88, positive_month_fraction=.6,
            rolling_63_sessions={"minimum": -.05})


def test_canonical_design_is_bound_in_full_including_no_promotion_and_initial_exemption():
    design = _design()
    experiment._check_design(design)
    assert design["arms"] == [{"key": k, "per_side_NAV_fraction": f} for k, f in experiment.CAPITAL_ARMS]
    assert design["budget"]["native_fits"] == design["budget"]["regenerated_predictions"] == 0
    assert design["automatic_promotion"] is False
    assert "first_scheduled_rebalance_batch_only" in design["capital_limit"]["initial_exemption"]


@pytest.mark.parametrize("section,key,value", [
    (None, "evaluation_end", "2026-10-16"), (None, "initial_cash_cny", "200000.00"),
    (None, "model", "newly_fitted_H20"), (None, "automatic_promotion", True),
    (None, "repair_old_Cinda_accounting", True), (None, "interpretation", []),
    ("unchanged", "top_k", 5), ("unchanged", "minimum_hold_sessions", 20),
    ("unchanged", "max_exit_intents_per_rebalance", 5), ("unchanged", "rebalance_actual_sessions", 20),
    ("capital_limit", "denominator", "execution_day_equity"),
    ("capital_limit", "initial_exemption", "whenever_all_cash"),
    ("capital_limit", "measurement", "requested_quantity_at_signal_close"),
    ("capital_limit", "rejected_allocation", "reallocate_on_second_pass"),
    ("decision", "average_invested_fraction_reduction_max", .5),
    ("decision", "preference_if_multiple_qualify", "highest_return"),
    ("budget", "native_fits", 1), ("budget", "reported_ledgers", 15),
])
def test_any_design_economic_scope_or_budget_change_rejected(section, key, value):
    design = _design()
    (design if section is None else design[section])[key] = value
    with pytest.raises(ValueError, match="entire predeclared"):
        experiment._check_design(design)


@pytest.mark.parametrize("mutate", ["new_key", "wrong_fraction", "fraction_float", "reorder", "missing_arm"])
def test_design_arm_grid_cannot_be_silently_extended_or_retyped(mutate):
    design = _design()
    if mutate == "new_key": design["after_result_note"] = True
    elif mutate == "wrong_fraction": design["arms"][1]["per_side_NAV_fraction"] = "0.06"
    elif mutate == "fraction_float": design["arms"][1]["per_side_NAV_fraction"] = .05
    elif mutate == "reorder": design["arms"].reverse()
    else: design["arms"].pop()
    with pytest.raises(ValueError): experiment._check_design(design)


def test_three_qualified_arms_prefer_largest_cap_not_highest_return_and_never_promote():
    rows = _paths()
    for key, profit in (("cap5pct", "2.0"), ("cap7p5pct", ".8"), ("cap10pct", ".2")):
        _qualify(rows, key)
        _row(rows, key)["metrics"]["total_return_lower_exact"] = profit
    decision = experiment.decide_capital_turnover(rows)
    assert decision["status"] == "RETROSPECTIVE_CANDIDATES"
    assert decision["qualified_arms"] == ["cap5pct", "cap7p5pct", "cap10pct"]
    assert decision["preferred_arm_for_further_review"] == "cap10pct"
    assert decision["current_paper_action"] == "keep_existing_top20_unchanged"
    assert decision["strategy_promoted"] is decision["model_retrained"] is False
    assert decision["historical_comparison_is_new_future_OOS"] is False


@pytest.mark.parametrize("gate", ["net_equal", "worse_dd", "equal_cost", "equal_turnover", "exposure_low",
    "positive_months_lower", "worst63_lower", "isolated_zero", "isolated_net_lower",
    "isolated_dd_worse", "isolated_cost_equal", "isolated_turnover_equal", "isolated_exposure_low",
    "risk_unknown", "single_over", "top3_over", "neff_low", "neff_missing"])
def test_each_formal_economic_stability_and_risk_gate_is_necessary(gate):
    rows = _paths(); _qualify(rows)
    continuous = _row(rows)["metrics"]
    isolated = _row(rows, period="2025")["metrics"]
    risk = _row(rows, period="2026")["diagnostics"]["risk"]
    if gate == "net_equal": continuous["total_return_lower_exact"] = ".10"
    elif gate == "worse_dd": continuous["max_drawdown_conservative_exact"] = "-.200001"
    elif gate == "equal_cost": continuous.update(total_fee_cny="1000", slippage_cost_cny="100")
    elif gate == "equal_turnover": continuous["one_way_turnover_average_nav"] = 10.0
    elif gate == "exposure_low": continuous["mean_invested_fraction"] = .84999
    elif gate == "positive_months_lower": continuous["positive_month_fraction"] = .49999
    elif gate == "worst63_lower": continuous["rolling_63_sessions"]["minimum"] = -.100001
    elif gate == "isolated_zero": isolated["total_return_lower_exact"] = "0"
    elif gate == "isolated_net_lower": isolated["total_return_lower_exact"] = ".099999"
    elif gate == "isolated_dd_worse": isolated["max_drawdown_conservative_exact"] = "-.20001"
    elif gate == "isolated_cost_equal": isolated.update(total_fee_cny="1000", slippage_cost_cny="100")
    elif gate == "isolated_turnover_equal": isolated["one_way_turnover_average_nav"] = 10.0
    elif gate == "isolated_exposure_low": isolated["mean_invested_fraction"] = .84999
    elif gate == "risk_unknown": risk.update(coverage_complete=False, max_single_nav_fraction=None,
        max_top3_nav_fraction=None, min_effective_stock_count=None)
    elif gate == "single_over": risk["max_single_nav_fraction"] = .250001
    elif gate == "top3_over": risk["max_top3_nav_fraction"] = .650001
    elif gate == "neff_low": risk["min_effective_stock_count"] = 3.9999
    else: risk["min_effective_stock_count"] = None
    decision = experiment.decide_capital_turnover(rows)
    assert decision["qualified_arms"] == []
    assert decision["preferred_arm_for_further_review"] is None
    assert decision["status"] == "NO_JOINT_IMPROVEMENT"


def test_inclusive_no_worse_boundaries_but_strict_continuous_gain_cost_and_turnover():
    rows = _paths(); _qualify(rows)
    for period in experiment.PERIODS:
        row = _row(rows, period=period)
        row["metrics"].update(total_return_lower_exact="0.100000001" if period == "continuous" else ".10",
            max_drawdown_conservative_exact="-.20", total_fee_cny="999.99", slippage_cost_cny="100",
            one_way_turnover_average_nav=9.999999, mean_invested_fraction=.85,
            positive_month_fraction=.5, rolling_63_sessions={"minimum": -.1})
        row["diagnostics"]["risk"].update(max_single_nav_fraction=.25,
            max_top3_nav_fraction=.65, min_effective_stock_count=4.0)
    decision = experiment.decide_capital_turnover(rows)
    assert decision["qualified_arms"] == ["cap5pct"]
    assert all(decision["comparisons"][0]["formal_checks"].values())


@pytest.mark.parametrize("invalid_period", experiment.PERIODS)
def test_invalid_reference_blocks_formal_gates_but_keeps_other_valid_paired_diagnostics(invalid_period):
    rows = _paths()
    for key, _ in experiment.CAPITAL_ARMS[1:]: _qualify(rows, key)
    _row(rows, "hold10_cap2", invalid_period)["performance_valid"] = False
    decision = experiment.decide_capital_turnover(rows)
    assert decision["status"] == "INCONCLUSIVE_INVALID_REFERENCE"
    assert decision["qualified_arms"] == [] and decision["preferred_arm_for_further_review"] is None
    for comparison in decision["comparisons"]:
        assert comparison["formal_checks"]["all_paths_valid"] is False
        assert all(value is None for key, value in comparison["formal_checks"].items() if key != "all_paths_valid")
        paired = comparison["paired_valid_period_diagnostics"]
        assert paired[invalid_period] is None
        assert all(all(checks.values()) for period, checks in paired.items() if period != invalid_period)
        assert comparison["qualified_for_further_prospective_evaluation"] is False


def test_invalid_candidate_only_disqualifies_its_own_formal_comparison():
    rows = _paths(); _qualify(rows); _qualify(rows, "cap10pct")
    _row(rows, period="2026")["performance_valid"] = False
    decision = experiment.decide_capital_turnover(rows)
    assert decision["qualified_arms"] == ["cap10pct"]
    assert decision["comparisons"][0]["paired_valid_period_diagnostics"]["2026"] is None


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unknown_arm", "wrong_period", "wrong_fraction", "float_fraction"])
def test_exactly_twelve_unique_declared_arm_fraction_period_combinations_required(mutation):
    rows = _paths()
    if mutation == "missing": rows.pop()
    elif mutation == "duplicate": rows.append(deepcopy(rows[0]))
    elif mutation == "unknown_arm": rows[-1]["arm_key"] = "cap15pct"
    elif mutation == "wrong_period": rows[-1]["period"] = "2026_10_16"
    elif mutation == "wrong_fraction": rows[-1]["fraction"] = "0.05"
    else: rows[-1]["fraction"] = .10
    with pytest.raises(ValueError): experiment.decide_capital_turnover(rows)


@pytest.fixture
def declaration_case(tmp_path, monkeypatch):
    reference, paper = tmp_path / "a6", tmp_path / "paper"
    files = {}
    for name in ("protocol", "market_audit", "merger_actions"):
        path = tmp_path / (name + ".json"); _write(path, {"synthetic": name})
        files[name] = experiment._proof(path)
    references = {}
    for period in experiment.PERIODS:
        folder = reference / "hold10_cap2" / period
        result = _reference(folder, period=period, valid=period != "continuous")
        result["metrics"].update(_metrics())
        summary = json.loads((folder / "summary.json").read_bytes()); summary["metrics"] = deepcopy(result["metrics"])
        _write(folder / "summary.json", summary)
        references[period] = result
    _write(reference / "report.json", {"kind": "a6_fixed_lgb_turnover_results", "all_performance_valid": False})
    _write(reference / "manifest.json", {"kind": "a6_fixed_lgb_turnover_results", "files": {
        str(p.relative_to(reference)): experiment._proof(p)["sha256"] for p in reference.rglob("*") if p.is_file()}})
    for p in reference.rglob("*"):
        if p.is_file(): files["reference:" + str(p.relative_to(reference))] = experiment._proof(p)
    active_code = tmp_path / "active-paper.py"; active_code.write_text("# unchanged active paper\n")
    _write(paper / "manifest.json", {"kind": "isolated_ashare_low_turnover_forward", "strategy": {"top_k": 20},
        "implementation_sha256": {str(active_code): experiment._proof(active_code)["sha256"]}})
    _write(paper / "state.json", {"cash": "100000.00", "positions": {}})
    files["active_paper_manifest"] = experiment._proof(paper / "manifest.json")
    files["active_code:" + str(active_code)] = experiment._proof(active_code)
    parent = {"kind": "a10_fixed_lgb_position_count_declaration", "source": {
        "snapshot": str(tmp_path / "snapshot"), "scope_directory": str(tmp_path / "scope"),
        "predictions_directory": str(tmp_path / "models")}, "bound_inputs": files,
        "reference_directory": str(reference), "active_paper": str(paper),
        "active_state_at_declaration": experiment._proof(paper / "state.json"),
        "active_state_is_observation_not_frozen_input": True, "fits_performed": 0,
        "predictions_regenerated": False}
    parent_path = tmp_path / "parent.json"; _write(parent_path, parent)
    design = tmp_path / "design.json"; _write(design, _design())
    code = {}
    for name in ("a11_experiment", "a11_validation", "a11_execution"):
        p = tmp_path / (name + ".py"); p.write_text("# synthetic frozen " + name + "\n"); code[name] = p
    calls = []
    monkeypatch.setattr(experiment, "implementation_proofs", lambda: {name: experiment._proof(p) for name, p in code.items()})
    monkeypatch.setattr(experiment, "verify_reused_stability_models", lambda *args: calls.append(("verify", args)) or {"status": "passed", "training_reperformed": False})
    monkeypatch.setattr(experiment, "load_stability_context", lambda *args: pytest.fail("Real sources must not be loaded by these synthetic gates"))
    return SimpleNamespace(arguments={"parent_declaration": parent_path, "design_path": design,
        "output": tmp_path / "declared.json"}, parent=parent, reference=reference, references=references,
        paper=paper, active_code=active_code, code=code, calls=calls, tmp=tmp_path)


def _freeze(case):
    return experiment.freeze_capital_turnover_declaration(**case.arguments)


def test_freeze_inherits_fixed_sources_and_every_reference_without_freezing_live_state(declaration_case):
    case = declaration_case
    before = {p: p.read_bytes() for p in case.paper.iterdir()}
    result = _freeze(case)
    assert result["source"] == case.parent["source"]
    assert result["reference_directory"] == case.parent["reference_directory"]
    assert result["active_paper"] == case.parent["active_paper"]
    assert all(result["bound_inputs"][key] == proof for key, proof in case.parent["bound_inputs"].items())
    assert result["fits_performed"] == 0 and result["predictions_regenerated"] is False
    assert result["active_state_is_observation_not_frozen_input"] is True
    assert result["active_state_at_declaration"] == experiment._proof(case.paper / "state.json")
    assert all(p.read_bytes() == raw for p, raw in before.items())
    assert len(case.calls) == 1 and case.calls[0][0] == "verify"


@pytest.mark.parametrize("target", ["active_code", "reference_trade", "protocol"])
def test_changed_inherited_source_or_production_bytes_rejected_before_freeze(declaration_case, target):
    case = declaration_case
    path = case.active_code if target == "active_code" else case.reference / "hold10_cap2/2025/trades.csv" if target == "reference_trade" else Path(case.parent["bound_inputs"]["protocol"]["path"])
    path.write_text("changed after parent declaration")
    with pytest.raises(ValueError): _freeze(case)
    assert not case.arguments["output"].exists() and case.calls == []


def test_freeze_never_overwrites_previous_declaration(declaration_case):
    case = declaration_case; case.arguments["output"].write_text("preserve")
    with pytest.raises(ValueError, match="Preserve"): _freeze(case)
    assert case.arguments["output"].read_text() == "preserve" and case.calls == []


@pytest.mark.parametrize("field", ["source", "reference_directory", "active_paper"])
def test_runtime_cannot_redirect_inherited_sources_even_with_unchanged_valid_bound_proofs(declaration_case, field):
    case = declaration_case; _freeze(case)
    path = case.arguments["output"]; payload = json.loads(path.read_bytes())
    if field == "source": payload[field]["snapshot"] = str(case.tmp / "different_snapshot")
    else: payload[field] = str(case.tmp / ("different_" + field))
    _write(path, payload)
    with pytest.raises(ValueError): experiment.run_capital_turnover_comparison(path, case.tmp / "results")
    assert not (case.tmp / "results").exists() and len(case.calls) == 1


@pytest.mark.parametrize("target", ["a11_validation", "a11_execution", "a11_experiment", "parent", "design", "reference", "active_code"])
def test_runtime_rejects_changed_proof_before_any_context_or_candidate(declaration_case, target):
    case = declaration_case; _freeze(case)
    path = case.code[target] if target in case.code else case.arguments["parent_declaration"] if target == "parent" else case.arguments["design_path"] if target == "design" else case.reference / "hold10_cap2/2025/trades.csv" if target == "reference" else case.active_code
    path.write_text("changed bound bytes")
    with pytest.raises(ValueError): experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results")
    assert not (case.tmp / "results").exists() and len(case.calls) == 1


def test_runtime_rejects_implementation_path_rebinding_and_existing_output(declaration_case, monkeypatch):
    case = declaration_case; _freeze(case)
    other = case.tmp / "other.py"; other.write_text("# other implementation")
    monkeypatch.setattr(experiment, "implementation_proofs", lambda: {"a11_experiment": experiment._proof(other)})
    with pytest.raises(ValueError, match="implementation changed"):
        experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results")
    target = case.tmp / "existing"; target.mkdir(); (target / "keep").write_text("original")
    with pytest.raises(ValueError, match="Preserve"):
        experiment.run_capital_turnover_comparison(case.arguments["output"], target)
    assert (target / "keep").read_text() == "original"


@pytest.mark.parametrize("where", ["paper_child", "paper_ancestor"])
def test_offline_freeze_and_run_outputs_cannot_touch_active_paper_tree(declaration_case, where):
    case = declaration_case; _freeze(case)
    unsafe = case.paper / "offline-results" if where == "paper_child" else case.paper.parent
    with pytest.raises(ValueError, match="separate|Preserve|immutable artifact"):
        experiment.freeze_capital_turnover_declaration(**{**case.arguments, "output": unsafe})
    with pytest.raises(ValueError, match="separate|Preserve|immutable artifact"):
        experiment.run_capital_turnover_comparison(case.arguments["output"], unsafe)
    assert not (case.paper / "offline-results").exists()
    assert len(case.calls) == 1


def test_nonexistent_output_ancestor_of_active_paper_is_rejected_directly(tmp_path):
    with pytest.raises(ValueError, match="separate"):
        experiment._check_output_isolation(tmp_path / "new-offline-parent", tmp_path / "new-offline-parent/paper")


@pytest.mark.parametrize("mutation", ["replace_valid_proof", "drop_inherited_proof"])
def test_runtime_cannot_rebind_or_drop_parent_proof_even_if_all_remaining_bytes_valid(declaration_case, mutation):
    case = declaration_case; _freeze(case)
    path = case.arguments["output"]; payload = json.loads(path.read_bytes())
    if mutation == "replace_valid_proof":
        other = case.tmp / "different-protocol.json"; _write(other, {"synthetic": "different but valid bytes"})
        payload["bound_inputs"]["protocol"] = experiment._proof(other)
    else:
        payload["bound_inputs"].pop("merger_actions")
    _write(path, payload)
    with pytest.raises(ValueError, match="inherited parent proof"):
        experiment.run_capital_turnover_comparison(path, case.tmp / "results")
    assert not (case.tmp / "results").exists() and len(case.calls) == 1


class _VerifiedInput:
    def __init__(self, calls, name): self.calls, self.name = calls, name
    @contextmanager
    def verified_batch(self):
        self.calls.append(("batch_enter", self.name))
        yield self
        self.calls.append(("batch_exit", self.name))
    def assert_unchanged(self): self.calls.append(("assert_unchanged", self.name))


@pytest.fixture
def synthetic_run(declaration_case, monkeypatch):
    case = declaration_case; _freeze(case)
    context = {"bound": {}, "pool": _VerifiedInput(case.calls, "pool"),
        "actions": _VerifiedInput(case.calls, "actions"), "score_coverage": {"stale": "not copied"}}
    def load(*args): case.calls.append(("load", args)); return context
    monkeypatch.setattr(experiment, "load_stability_context", load)
    def simulate(context, *, period, fraction):
        assert "score_coverage" not in context
        case.calls.append(("simulate", fraction, period))
        result = deepcopy(case.references[period])
        if fraction is not None:
            result["records"]["capital_turnover"] = [{"session": "2026-10-19", "fraction": fraction,
                "initial_build": False, "frozen_quantities": {"SH600000": 100},
                "source_checked": True, "reserved_gross_cny": "1000.00"}]
        return result
    monkeypatch.setattr(experiment, "simulate_capital_turnover", simulate)
    from stopat30m import ashare_position_count_metrics
    monkeypatch.setattr(ashare_position_count_metrics, "position_count_diagnostics", lambda result: {
        "risk": _risk(), "daily_exposures": [{"date": "2026-10-19", "coverage_complete": True}]})
    case.simulate, case.context = simulate, context
    return case


def test_mock_run_reproduces_three_full_baselines_before_nine_candidates_and_exports_all_bytes(synthetic_run):
    case = synthetic_run; before = {p: p.read_bytes() for p in case.paper.iterdir()}; progress = []
    result = experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results", progress=progress.append)
    calls = [call[1:] for call in case.calls if call[0] == "simulate"]
    assert calls == [(fraction, period) for _, fraction in experiment.CAPITAL_ARMS for period in experiment.PERIODS]
    assert set(result["baseline_full_reproduction"]) == set(experiment.PERIODS)
    assert all(r["passed"] and len(r["record_types"]) == 13 for r in result["baseline_full_reproduction"].values())
    assert len(result["results"]) == 12
    assert result["all_performance_valid"] is False
    assert result["native_fits"] == result["live_broker_orders"] == 0
    assert result["predictions_regenerated"] is result["active_paper_changed_by_experiment"] is False
    assert result["decision"]["status"] == "INCONCLUSIVE_INVALID_REFERENCE"
    assert all(p.read_bytes() == raw for p, raw in before.items())
    output = case.tmp / "results"
    assert (output / "declaration.json").read_bytes() == case.arguments["output"].read_bytes()
    assert json.loads((output / "status.json").read_bytes())["phase"] == "completed"
    for arm, fraction in experiment.CAPITAL_ARMS:
        for period in experiment.PERIODS:
            folder = output / arm / period
            summary = json.loads((folder / "summary.json").read_bytes())
            assert summary["arm_key"] == arm and summary["fraction"] == fraction
            assert "daily_exposures" not in summary["diagnostics"]
            assert json.loads((folder / "diagnostics.json").read_bytes())["daily_exposures"]
            if fraction is None: assert not (folder / "capital_turnover.json").exists()
            else:
                guard = json.loads((folder / "capital_turnover.json").read_bytes())[0]
                assert guard["fraction"] == fraction and guard["initial_build"] is False
                assert guard["frozen_quantities"] == {"SH600000": 100} and guard["source_checked"] is True
    inventory = json.loads((output / "manifest.json").read_bytes())["files"]
    assert set(inventory) == {str(p.relative_to(output)) for p in output.rglob("*") if p.is_file() and p.name != "manifest.json"}
    assert all(hashlib.sha256((output / name).read_bytes()).hexdigest() == sha for name, sha in inventory.items())
    assert len([call for call in case.calls if call[0] == "verify"]) == 3
    assert len([row for row in progress if row.get("completed")]) == 12
    assert case.context["score_coverage"] == {"stale": "not copied"}


@pytest.mark.parametrize("record", ["curves", "trades", "orders", "positions", "claims", "signals",
    "corporate_actions", "plans", "valuation_audit", "score_coverage", "holding_age",
    "holding_origin_actions", "turnover_plans"])
def test_any_baseline_record_mismatch_stops_before_all_candidate_calls(synthetic_run, monkeypatch, record):
    case = synthetic_run
    def bad(*args, **kwargs):
        result = case.simulate(*args, **kwargs)
        if kwargs["period"] == "2026" and kwargs["fraction"] is None:
            result["records"][record][0]["changed_without_NAV_change"] = True
        return result
    monkeypatch.setattr(experiment, "simulate_capital_turnover", bad)
    with pytest.raises(ValueError, match="full (CSV|JSON)"):
        experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results")
    assert all(call[1] is None for call in case.calls if call[0] == "simulate")
    failure = json.loads((case.tmp / "results/failure.json").read_bytes())
    assert failure["no_valid_comparison_claim"] is True and failure["completed_paths"] == 1
    assert not (case.tmp / "results/report.json").exists()


def test_missing_candidate_guard_audit_cannot_be_exported_as_a_completed_path(synthetic_run, monkeypatch):
    case = synthetic_run
    def bad(*args, **kwargs):
        result = case.simulate(*args, **kwargs)
        if kwargs["fraction"] is not None: result["records"]["capital_turnover"] = []
        return result
    monkeypatch.setattr(experiment, "simulate_capital_turnover", bad)
    with pytest.raises(ValueError, match="Every candidate rebalance"):
        experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results")
    failure = json.loads((case.tmp / "results/failure.json").read_bytes())
    assert failure["completed_paths"] == 3 and set(failure["reproduced_periods"]) == set(experiment.PERIODS)
    assert not (case.tmp / "results/report.json").exists()


def test_midrun_proof_change_is_caught_after_simulation_before_a_completed_report(synthetic_run):
    case = synthetic_run
    def progress(row):
        if row.get("completed") and row["arm_key"] == "cap10pct" and row["period"] == "continuous":
            case.code["a11_execution"].write_text("# changed during synthetic run")
    with pytest.raises(ValueError):
        experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results", progress=progress)
    assert len([call for call in case.calls if call[0] == "simulate"]) == 12
    assert not (case.tmp / "results/report.json").exists() and not (case.tmp / "results/manifest.json").exists()
    assert json.loads((case.tmp / "results/status.json").read_bytes())["phase"] == "failed"


def test_current_paper_state_is_a_live_observation_not_a_frozen_runtime_dependency(synthetic_run):
    case = synthetic_run
    _write(case.paper / "state.json", {"cash": "99900.00", "new_legitimate_receipt": "synthetic next session"})
    result = experiment.run_capital_turnover_comparison(case.arguments["output"], case.tmp / "results")
    assert result["active_paper_changed_by_experiment"] is False
    assert json.loads((case.paper / "state.json").read_bytes())["cash"] == "99900.00"


def test_invalid_cli_result_exits_once_without_automatic_paper_switch(tmp_path, monkeypatch, capsys):
    rows = _paths(); _row(rows, "hold10_cap2")["performance_valid"] = False
    decision = experiment.decide_capital_turnover(rows); calls = []
    monkeypatch.setattr(experiment, "run_capital_turnover_comparison", lambda *args, **kwargs: calls.append(args) or {
        "all_performance_valid": False, "decision": decision})
    monkeypatch.setattr(sys, "argv", ["a11", "run", "--declaration", str(tmp_path / "sealed.json"),
        "--output-dir", str(tmp_path / "unused")])
    with pytest.raises(SystemExit) as stopped: experiment.main()
    assert stopped.value.code == 1 and len(calls) == 1
    assert "INCONCLUSIVE_INVALID_REFERENCE" in capsys.readouterr().out
    assert not (tmp_path / "unused").exists()
