"""A10 declaration, exact-ledger and decision checks; no historical run or fit."""
from copy import deepcopy
import json
from pathlib import Path
import sys

import pandas as pd
import pytest

from stopat30m import ashare_position_count_experiment as experiment


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True))


def _reference(directory, *, period="continuous", valid=False):
    records = {
        "curves": [{"date": "2026-10-19", "equity_cny": "101000.00", "accounting_valid": valid}],
        "trades": [{"instrument": "SH600000", "quantity": 100, "commission": "5.00"},
                   {"instrument": "SZ000001", "quantity": 200, "commission": "6.00"}],
        "orders": [{"instrument": "SH600000", "status": "rejected", "reason": "limit"}],
        "positions": [{"instrument": "SH601059", "quantity": 300, "last_mark": "15.56"}],
        "claims": [{"instrument": "SH601059", "kind": "unresolved_delivery", "quantity": "300"}],
        "signals": [{"session": "2026-10-19", "instrument": "SH600000", "selected": True}],
        "corporate_actions": [{"instrument": "SH600000", "kind": "cash_dividend", "tax": "1.00"}],
        "plans": [{"session": "2026-10-19", "retained": ["SH601059"], "entries": []}],
        "valuation_audit": [{"session": "2026-10-19", "accounting_valid": valid,
                             "unpriced_delisted_claims": [] if valid else ["SH601059"]}],
        "score_coverage": [{"signal_date": "2026-10-16", "members": 1300, "missing_required_scores": []}],
        "holding_age": [{"instrument": "SH601059", "holding_age_sessions": 30}],
        "holding_origin_actions": [{"instrument": "SH601059", "kind": "actual_first_buy", "origin": "2026-09-01"}],
        "turnover_plans": [{"allowed_exits": ["SH600000"], "holding_age_sessions": {"SH601059": 30}}],
    }
    result = {"period": period, "model": "LGB", "variant": deepcopy(experiment.HOLD_RULE),
        "execution_id": "E2", "performance_valid": valid,
        "metrics_basis": "declared_accounting_NAV_not_actual_liquidation" if valid else "invalid_indicative_carried_ledger_do_not_use_for_selection",
        "metrics": {"total_return_lower_exact": "0.01", "accounting_invalid_days": 0 if valid else 1},
        "stopped": None, "actual_last_valued_session": "2026-10-19", "records": records,
        "final_state": {"cash": "500.00", "positions": {"SH601059": {"quantity": 300}}, "claims": {"physical": "pending"}}}
    directory.mkdir(parents=True, exist_ok=True)
    for name, rows in records.items():
        if name in experiment.JSON_RECORDS:
            _write(directory / (name + ".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
    summary = {key: deepcopy(value) for key, value in result.items() if key not in {"records", "final_state"}}
    summary.update(variant="hold10_cap2", variant_terms=deepcopy(experiment.HOLD_RULE),
        score_quality={"rank_ic_mean": .06}, score_quality_is_fixed_universe_diagnostic=True,
        minimum_commission_fill_fraction=.5)
    _write(directory / "summary.json", summary)
    _write(directory / "final_state.json", result["final_state"])
    _write(directory / "score_quality.json", summary["score_quality"])
    return result


@pytest.fixture
def reference(tmp_path):
    folder = tmp_path / "reference"
    return _reference(folder), folder


def test_full_thirteen_record_invalid_reference_reproduces_without_reclassifying(reference):
    result, folder = reference
    receipt = experiment.assert_reference_reproduced(result, folder)
    assert receipt["passed"] and receipt["performance_valid"] is False
    assert len(receipt["record_types"]) == 13
    assert receipt["fixed_score_quality_reused_not_recomputed"] == experiment._proof(folder / "score_quality.json")
    assert result["records"]["valuation_audit"][0]["unpriced_delisted_claims"] == ["SH601059"]


@pytest.mark.parametrize("key,value", [
    ("period", "2025"), ("model", "H20"), ("execution_id", "E3"), ("performance_valid", True),
    ("metrics_basis", "now_valid"), ("metrics", {"total_return_lower_exact": "0.02"}),
    ("stopped", {"reason": "funding"}), ("actual_last_valued_session", "2026-10-16"),
    ("variant", {"key": "hold10_cap2", "minimum_hold_sessions": 5, "max_replacements": 2}),
])
def test_any_core_summary_change_is_rejected(reference, key, value):
    original, folder = reference
    result = deepcopy(original)
    result[key] = value
    with pytest.raises(ValueError, match="core summary"):
        experiment.assert_reference_reproduced(result, folder)


@pytest.mark.parametrize("where", ["actual_extra", "actual_missing", "reference_extra"])
def test_finite_metadata_whitelist_does_not_silently_drop_other_summary_keys(reference, where):
    original, folder = reference
    result = deepcopy(original)
    if where == "actual_extra":
        result["unknown_core_diagnostic"] = 7
    elif where == "actual_missing":
        result.pop("stopped")
    else:
        path = folder / "summary.json"
        expected = json.loads(path.read_text())
        expected["unknown_core_diagnostic"] = 7
        _write(path, expected)
    with pytest.raises(ValueError, match="core summary"):
        experiment.assert_reference_reproduced(result, folder)


def test_exported_fixed_score_quality_is_bound_but_not_recomputed_or_an_economic_field(reference):
    result, folder = reference
    path = folder / "summary.json"
    expected = json.loads(path.read_text())
    expected["score_quality"] = {"rank_ic_mean": .99, "only_old_export_metadata": True}
    _write(path, expected)
    receipt = experiment.assert_reference_reproduced(result, folder)
    assert receipt["fixed_score_quality_reused_not_recomputed"] == experiment._proof(folder / "score_quality.json")


def test_minimum_commission_fraction_is_checked_against_exact_fills(reference):
    result, folder = reference
    path = folder / "summary.json"
    expected = json.loads(path.read_text())
    expected["minimum_commission_fill_fraction"] = 1.0
    _write(path, expected)
    with pytest.raises(ValueError, match="minimum-commission"):
        experiment.assert_reference_reproduced(result, folder)


@pytest.mark.parametrize("name", ["curves", "trades", "orders", "positions", "claims", "signals",
    "corporate_actions", "plans", "valuation_audit", "score_coverage", "holding_age", "holding_origin_actions", "turnover_plans"])
def test_every_canonical_record_is_compared_even_when_nav_is_unchanged(reference, name):
    original, folder = reference
    result = deepcopy(original)
    result["records"][name][0]["tampered_without_nav_change"] = True
    with pytest.raises(ValueError, match="full (?:CSV|JSON)"):
        experiment.assert_reference_reproduced(result, folder)


@pytest.mark.parametrize("change", ["missing", "extra", "reordered_csv", "final_claim"])
def test_inventory_csv_serialization_and_entire_final_state_are_strict(reference, change):
    original, folder = reference
    result = deepcopy(original)
    if change == "missing":
        result["records"].pop("holding_origin_actions")
    elif change == "extra":
        result["records"]["unlisted"] = []
    elif change == "reordered_csv":
        frame = pd.read_csv(folder / "trades.csv")
        frame[frame.columns[::-1]].to_csv(folder / "trades.csv", index=False)
    else:
        result["final_state"]["claims"] = {}
    with pytest.raises(ValueError, match="Top20"):
        experiment.assert_reference_reproduced(result, folder)


def _paths():
    return [{"target_count": count, "period": period, "performance_valid": True,
        "metrics": {"total_return_lower_exact": "0.10", "max_drawdown_conservative_exact": "-0.20",
            "total_fee_cny": "1000.00", "slippage_cost_cny": "100.00", "one_way_turnover_average_nav": 10.0,
            "positive_month_fraction": .5, "rolling_63_sessions": {"minimum": -.1}},
        "diagnostics": {"risk": {"coverage_complete": True, "mean_risky_nav_fraction": .9,
            "max_single_nav_fraction": .1, "max_top3_nav_fraction": .3, "min_effective_stock_count": 10.0}}}
        for count in experiment.TARGET_COUNTS for period in experiment.PERIODS]


def _row(rows, count, period="continuous"):
    return next(row for row in rows if row["target_count"] == count and row["period"] == period)


def _qualify(rows, count):
    for period in experiment.PERIODS:
        row = _row(rows, count, period)
        row["metrics"].update(total_return_lower_exact="0.20", max_drawdown_conservative_exact="-0.15",
            total_fee_cny="800.00", slippage_cost_cny="50.00", one_way_turnover_average_nav=8.0,
            positive_month_fraction=.6, rolling_63_sessions={"minimum": -.05})
        row["diagnostics"]["risk"].update(mean_risky_nav_fraction=.88,
            max_single_nav_fraction=.2, max_top3_nav_fraction=.6, min_effective_stock_count=5.0)


def test_multiple_qualified_counts_choose_largest_n_never_highest_return_or_change_paper():
    rows = _paths()
    for count, profit in ((15, "0.2"), (10, "0.8"), (5, "2.0")):
        _qualify(rows, count)
        _row(rows, count)["metrics"]["total_return_lower_exact"] = profit
    decision = experiment.decide_position_counts(rows)
    assert decision["qualified_counts"] == [15, 10, 5]
    assert decision["preferred_count_for_further_review"] == 15
    assert decision["status"] == "RETROSPECTIVE_CANDIDATES"
    assert decision["current_paper_action"] == "keep_existing_top20_unchanged"
    assert decision["strategy_promoted"] is decision["model_retrained"] is False


@pytest.mark.parametrize("gate", ["net_equal", "worse_dd", "equal_cost", "higher_turnover", "year_zero", "year_loss",
    "lower_positive_months", "worse_63", "missing_risk", "single_over", "top3_over", "neff_below", "neff_missing", "exposure_low"])
def test_each_independent_economic_stability_and_risk_gate_is_required(gate):
    rows = _paths()
    _qualify(rows, 15)
    metrics, risk = _row(rows, 15)["metrics"], _row(rows, 15)["diagnostics"]["risk"]
    if gate == "net_equal": metrics["total_return_lower_exact"] = "0.10"
    elif gate == "worse_dd": metrics["max_drawdown_conservative_exact"] = "-0.20001"
    elif gate == "equal_cost": metrics.update(total_fee_cny="1000", slippage_cost_cny="100")
    elif gate == "higher_turnover": metrics["one_way_turnover_average_nav"] = 10.0001
    elif gate in {"year_zero", "year_loss"}:
        _row(rows, 15, "2026")["metrics"]["total_return_lower_exact"] = "0" if gate == "year_zero" else "-0.01"
    elif gate == "lower_positive_months": metrics["positive_month_fraction"] = .4999
    elif gate == "worse_63": metrics["rolling_63_sessions"]["minimum"] = -.10001
    elif gate == "missing_risk": _row(rows, 15, "2025")["diagnostics"]["risk"]["coverage_complete"] = False
    elif gate == "single_over": _row(rows, 15, "2025")["diagnostics"]["risk"]["max_single_nav_fraction"] = .25001
    elif gate == "top3_over": _row(rows, 15, "2026")["diagnostics"]["risk"]["max_top3_nav_fraction"] = .65001
    elif gate == "neff_below": risk["min_effective_stock_count"] = 3.9999
    elif gate == "neff_missing": risk["min_effective_stock_count"] = None
    elif gate == "exposure_low": risk["mean_risky_nav_fraction"] = .8499
    decision = experiment.decide_position_counts(rows)
    assert decision["qualified_counts"] == []
    assert decision["preferred_count_for_further_review"] is None
    assert decision["status"] == "NO_JOINT_IMPROVEMENT"


def test_no_worse_risk_boundaries_are_inclusive_but_profit_and_cost_are_strict():
    rows = _paths()
    _qualify(rows, 15)
    metrics = _row(rows, 15)["metrics"]
    metrics.update(total_return_lower_exact="0.100000001", max_drawdown_conservative_exact="-0.20",
        total_fee_cny="999.99", slippage_cost_cny="100.00", one_way_turnover_average_nav=10.0,
        positive_month_fraction=.5, rolling_63_sessions={"minimum": -.1})
    for period in experiment.PERIODS:
        _row(rows, 15, period)["diagnostics"]["risk"].update(mean_risky_nav_fraction=.85,
            max_single_nav_fraction=.25, max_top3_nav_fraction=.65, min_effective_stock_count=4.0)
    decision = experiment.decide_position_counts(rows)
    assert decision["qualified_counts"] == [15]
    assert all(next(c for c in decision["comparisons"] if c["target_count"] == 15)["checks"].values())


@pytest.mark.parametrize("period", experiment.PERIODS)
def test_any_invalid_reference_is_inconclusive_even_with_apparently_large_candidate_profit(period):
    rows = _paths()
    for count in experiment.TARGET_COUNTS[1:]: _qualify(rows, count)
    _row(rows, 20, period)["performance_valid"] = False
    decision = experiment.decide_position_counts(rows)
    assert decision["status"] == "INCONCLUSIVE_INVALID_REFERENCE"
    assert decision["qualified_counts"] == [] and decision["preferred_count_for_further_review"] is None
    for comparison in decision["comparisons"]:
        assert comparison["checks"]["all_candidate_and_reference_paths_valid"] is False
        assert all(value is None for key, value in comparison["checks"].items()
                   if key != "all_candidate_and_reference_paths_valid")
    assert decision["current_paper_action"] == "keep_existing_top20_unchanged"


def test_invalid_candidate_does_not_invalidate_other_candidate_comparisons():
    rows = _paths()
    _qualify(rows, 15); _qualify(rows, 10)
    _row(rows, 15, "2025")["performance_valid"] = False
    decision = experiment.decide_position_counts(rows)
    assert decision["qualified_counts"] == [10] and decision["preferred_count_for_further_review"] == 10


@pytest.mark.parametrize("change", ["missing", "duplicate", "extra", "wrong_period"])
def test_all_twelve_unique_predeclared_paths_required(change):
    rows = _paths()
    if change == "missing": rows.pop()
    elif change == "duplicate": rows.append(deepcopy(rows[0]))
    elif change == "extra": rows[-1]["target_count"] = 8
    else: rows[-1]["period"] = "through_2026_10_16"
    with pytest.raises(ValueError, match="twelve.*exactly once"):
        experiment.decide_position_counts(rows)


def _design():
    return json.loads((Path(__file__).parents[1] / "docs/research/a10_position_count_design_v1.json").read_bytes())


@pytest.mark.parametrize("section,key,value", [
    (None, "target_counts", [20, 15, 10, 8]), (None, "periods", ["2025", "continuous"]),
    (None, "initial_cash_cny", "200000.00"), (None, "model", "A8_H20"),
    (None, "evaluation_end", "2026-10-16"), (None, "automatic_promotion", True),
    (None, "repair_old_Cinda_accounting", True), (None, "only_changed_input", "model_and_top_k"),
    ("unchanged", "minimum_hold_sessions", 20), ("unchanged", "max_exit_intents_per_rebalance", 5),
    ("unchanged", "keep_rank", 30), ("unchanged", "rebalance_actual_sessions", 20),
    ("decision", "max_single_security_NAV_fraction", .5), ("decision", "minimum_effective_stock_count_on_invested_days", 2),
    ("budget", "native_fits", 1), ("budget", "reported_ledgers", 15),
])
def test_frozen_design_cannot_claim_different_model_budget_rule_risk_or_endpoint(section, key, value):
    design = _design()
    experiment._check_design(design)
    (design if section is None else design[section])[key] = value
    with pytest.raises(ValueError):
        experiment._check_design(design)


@pytest.fixture
def declaration_case(tmp_path, monkeypatch):
    reference, paper = tmp_path / "a6", tmp_path / "paper"
    protocol, audit = tmp_path / "protocol.json", tmp_path / "audit.json"
    _write(protocol, {"synthetic": "immutable original protocol"})
    _write(audit, {"synthetic": "immutable original audit"})
    parent = {"kind": "a6_fixed_lgb_turnover_declaration", "snapshot": str(tmp_path / "snapshot"),
        "scope_directory": str(tmp_path / "scope"), "predictions_directory": str(tmp_path / "models"),
        "bound_inputs": {"protocol": experiment._proof(protocol), "market_audit": experiment._proof(audit)}}
    parent_path = tmp_path / "parent.json"
    _write(parent_path, parent); _write(reference / "declaration.json", parent)
    for period in experiment.PERIODS: _reference(reference / "hold10_cap2" / period, period=period, valid=period != "continuous")
    _write(reference / "report.json", {"kind": "a6_fixed_lgb_turnover_results", "all_performance_valid": False})
    _write(reference / "manifest.json", {"kind": "a6_fixed_lgb_turnover_results", "files": {
        str(p.relative_to(reference)): experiment._proof(p)["sha256"] for p in reference.rglob("*") if p.is_file()}})
    code = tmp_path / "frozen-a10.py"; code.write_text("# synthetic fixed implementation\n")
    active_code = tmp_path / "active-paper.py"; active_code.write_text("# unchanged active paper\n")
    _write(paper / "manifest.json", {"kind": "isolated_ashare_low_turnover_forward", "strategy": {"top_k": 20},
        "implementation_sha256": {str(active_code): experiment._proof(active_code)["sha256"]}})
    _write(paper / "state.json", {"cash": "100000.00", "positions": {}})
    design = tmp_path / "design.json"; _write(design, _design())
    calls = []
    monkeypatch.setattr(experiment, "implementation_proofs", lambda: {"a10_experiment": experiment._proof(code)})
    monkeypatch.setattr(experiment, "verify_reused_stability_models", lambda *args: calls.append(args) or {"status": "passed", "training_reperformed": False})
    monkeypatch.setattr(experiment, "load_stability_context", lambda *args: pytest.fail("No real context/simulation may run in these declaration guard tests"))
    return dict(parent_declaration=parent_path, reference_directory=reference, design_path=design,
                active_paper=paper, output=tmp_path / "declared.json"), code, active_code, calls


def test_lightweight_declaration_accepts_invalid_reference_binds_every_reference_and_preserves_paper(declaration_case):
    arguments, _, _, calls = declaration_case
    state_path = arguments["active_paper"] / "state.json"
    before = state_path.read_bytes()
    result = experiment.freeze_position_count_declaration(**arguments)
    assert result["fits_performed"] == 0 and result["predictions_regenerated"] is False
    assert result["active_state_is_observation_not_frozen_input"] is True and state_path.read_bytes() == before
    assert len([name for name in result["bound_inputs"] if name.startswith("reference:")]) == 48
    assert len(calls) == 1


def test_modified_bound_code_rejected_before_context_or_ledger_runs(declaration_case, tmp_path):
    arguments, code, _, calls = declaration_case
    experiment.freeze_position_count_declaration(**arguments)
    code.write_text("# changed after freeze\n")
    with pytest.raises(ValueError):
        experiment.run_position_count_comparison(arguments["output"], tmp_path / "new-results")
    assert len(calls) == 1 and not (tmp_path / "new-results").exists()


def test_rebinding_implementation_path_is_rejected_even_if_original_bytes_still_match(declaration_case, tmp_path, monkeypatch):
    arguments, _, _, calls = declaration_case
    experiment.freeze_position_count_declaration(**arguments)
    other = tmp_path / "different-code.py"; other.write_text("# a different runtime implementation\n")
    monkeypatch.setattr(experiment, "implementation_proofs", lambda: {"a10_experiment": experiment._proof(other)})
    with pytest.raises(ValueError, match="implementation changed"):
        experiment.run_position_count_comparison(arguments["output"], tmp_path / "new-results")
    assert len(calls) == 1


def test_existing_output_and_changed_active_production_code_are_never_accepted(declaration_case):
    arguments, _, active_code, _ = declaration_case
    active_code.write_text("# unexpected production change\n")
    with pytest.raises(ValueError):
        experiment.freeze_position_count_declaration(**arguments)
    assert not arguments["output"].exists()
    arguments["output"].write_text("preserve unrelated existing declaration")
    with pytest.raises(ValueError, match="Preserve"):
        experiment.freeze_position_count_declaration(**arguments)


def test_invalid_cli_report_exits_nonzero_without_promoting_or_rerunning_a_model(tmp_path, monkeypatch, capsys):
    rows = _paths()
    _qualify(rows, 15)
    _row(rows, 20)["performance_valid"] = False
    decision = experiment.decide_position_counts(rows)
    calls = []
    monkeypatch.setattr(experiment, "run_position_count_comparison",
        lambda *args, **kwargs: calls.append(args) or {"all_performance_valid": False, "decision": decision})
    monkeypatch.setattr(sys, "argv", ["a10", "run", "--declaration", str(tmp_path / "sealed.json"),
                                    "--output-dir", str(tmp_path / "unused-results")])
    with pytest.raises(SystemExit) as stopped:
        experiment.main()
    assert stopped.value.code == 1 and len(calls) == 1
    assert "INCONCLUSIVE_INVALID_REFERENCE" in capsys.readouterr().out
    assert not (tmp_path / "unused-results").exists()
