"""Frozen, offline position-count trials; never imports an active paper runner."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd

from stopat30m.ashare_position_count_validation import TARGET_COUNTS, HOLD_RULE, simulate_position_count
from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.ashare_stability_validation import _assert_proofs, _proof, _save, load_stability_context
from stopat30m.ashare_turnover_experiment import JSON_RECORDS, PERIODS
from stopat30m.data.qlib_snapshot import guard_qlib_output_path

KIND = "a10_fixed_lgb_position_count_declaration"


def _read(path):
    return json.loads(Path(path).read_bytes())


def implementation_proofs():
    from stopat30m import ashare_position_count_validation, ashare_position_count_metrics
    return {name: _proof(path) for name, path in {
        "a10_experiment": __file__, "a10_validation": ashare_position_count_validation.__file__,
        "a10_metrics": ashare_position_count_metrics.__file__,
    }.items()}


def _check_design(design):
    if (design.get("kind") != "a10_fixed_lgb_position_count_design"
            or design.get("any_new_historical_candidate_returns_computed") is not False
            or design.get("target_counts") != list(TARGET_COUNTS)
            or design.get("periods") != list(PERIODS)
            or design.get("evaluation_end") != "2026-10-19"
            or design.get("automatic_promotion") is not False
            or design.get("active_paper_action") != "continue_existing_top20_unchanged"):
        raise ValueError("The predeclared four-count offline design is required")
    fixed = {"schema_version": 1, "initial_cash_cny": "100000.00",
        "only_changed_input": "prior_close_rank_planner_target_top_k",
        "model": "unchanged_original_A5_annual_H5_LGB_2025_and_LGB_2026_predictions",
        "repair_old_Cinda_accounting": False}
    if any(type(design.get(key)) is not type(value) or design.get(key) != value
           for key, value in fixed.items()):
        raise ValueError("Keep the fixed original-model, capital and no-repair scope")
    terms = design.get("unchanged", {})
    if terms != {
        "keep_rank": 40, "minimum_hold_sessions": 10,
        "max_exit_intents_per_rebalance": 2, "rebalance_actual_sessions": 5,
        "fees": "original_5_CNY_minimum_0.0003_commission_transfer_historical_stamp_tax",
        "slippage": "original_2_bps_adverse_plus_0.01_price_tick",
        "execution": "same_whole_lots_T1_price_limits_halts_no_reselection_no_retained_reweight",
        "history": "same_actual_signal_date_CSI300_and_CSI1000_members_and_corporate_action_sources",
        "continuous_account": "no_yearly_cash_reset_or_rebalance_phase_reset"}:
        raise ValueError("Only position count may change; retain the original turnover terms")
    gates = design.get("decision", {})
    expected = {"qualification": "candidate_and_top20_all_three_paths_valid",
        "continuous_net_return": "strictly_higher", "continuous_conservative_drawdown": "no_worse",
        "continuous_actual_fees_plus_slippage": "strictly_lower", "continuous_actual_turnover": "no_higher",
        "isolated_2025_and_2026_net_returns": "both_positive",
        "continuous_positive_month_fraction": "no_lower", "continuous_worst_63_session_return": "no_worse",
        "mean_risky_NAV_exposure_reduction_max": .05, "max_single_security_NAV_fraction": .25,
        "max_top3_security_NAV_fraction": .65, "minimum_effective_stock_count_on_invested_days": 4,
        "concentration_data": "complete_required_cash_not_a_diversifying_security",
        "tie_break": "largest_target_count_among_qualified_not_highest_historical_return",
        "risk_threshold_origin": "predeclared_research_preferences_not_empirical_confidence_or_guarantees",
        "any_invalid_baseline_path": "inconclusive_invalid_preserve_all_results_no_switch"}
    if gates != expected:
        raise ValueError("The declared gates differ from the implemented offline decision")
    if design.get("initial_cash_cny") != "100000.00" or design.get("budget") != {
        "reported_ledgers": 12, "baseline_top20_full_paths_before_other_arms": 3,
        "native_fits": 0, "regenerated_predictions": 0, "post_result_grid_expansion": False}:
        raise ValueError("Keep the original capital and finite offline experiment budget")


def freeze_position_count_declaration(*, parent_declaration, reference_directory,
        design_path, active_paper, output):
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve previous declarations")
    parent_path, reference = Path(parent_declaration).resolve(), Path(reference_directory).resolve()
    parent, design = _read(parent_path), _read(design_path)
    _check_design(design)
    if parent.get("kind") != "a6_fixed_lgb_turnover_declaration":
        raise ValueError("The original A6 source/prediction declaration is required")
    if _read(reference / "declaration.json") != parent:
        raise ValueError("Reference ledgers belong to another parent declaration")
    reference_manifest = _read(reference / "manifest.json")
    if reference_manifest.get("kind") != "a6_fixed_lgb_turnover_results":
        raise ValueError("Original A6 result manifest is required, including invalid paths")
    bound = {**parent["bound_inputs"], **implementation_proofs(),
        "a10_design": _proof(design_path), "a6_parent": _proof(parent_path),
        "a6_result_manifest": _proof(reference / "manifest.json"),
        "a6_result_report": _proof(reference / "report.json")}
    for name, digest in reference_manifest["files"].items():
        if not name.startswith("hold10_cap2/"):
            continue
        path = (reference / name).resolve()
        if not path.is_relative_to(reference):
            raise ValueError("Reference ledger path escapes its directory")
        bound["reference:" + name] = {"path": str(path), "sha256": digest}
    paper = Path(active_paper).resolve()
    active = _read(paper / "manifest.json")
    if active.get("kind") != "isolated_ashare_low_turnover_forward" or active["strategy"]["top_k"] != 20:
        raise ValueError("Expected the existing isolated top20 paper to remain unchanged")
    bound["active_paper_manifest"] = _proof(paper / "manifest.json")
    for name, digest in active["implementation_sha256"].items():
        bound["active_code:" + name] = {"path": name, "sha256": digest}
    _assert_proofs(bound)
    verified = verify_reused_stability_models(parent["predictions_directory"],
        bound["protocol"]["path"], bound["market_audit"]["path"])
    payload = {"kind": KIND, "declared_at": datetime.now(timezone.utc).isoformat(),
        "design": design, "bound_inputs": bound, "reference_directory": str(reference),
        "source": {key: parent[key] for key in ("snapshot", "scope_directory", "predictions_directory")},
        "model_verification": verified, "active_paper": str(paper),
        "active_state_at_declaration": _proof(paper / "state.json"),
        "active_state_is_observation_not_frozen_input": True,
        "fits_performed": 0, "predictions_regenerated": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    _save(output, payload)
    return payload


def assert_reference_reproduced(result, directory):
    """Exact A6 simulation outputs, with a finite export-only normalization."""
    directory = Path(directory)
    expected = _read(directory / "summary.json")
    if expected["variant"] != HOLD_RULE["key"] or expected["variant_terms"] != HOLD_RULE:
        raise ValueError("Expected the original hold10_cap2 reference")
    actual = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
    normalized = deepcopy(expected)
    normalized["variant"] = normalized.pop("variant_terms")
    for key in ("score_quality", "score_quality_is_fixed_universe_diagnostic", "minimum_commission_fill_fraction"):
        normalized.pop(key)
    if actual != normalized or result["final_state"] != _read(directory / "final_state.json"):
        raise ValueError("Top20 core summary or final state differs from the full A6 reference")
    trades = result["records"]["trades"]
    fraction = sum(Decimal(row["commission"]) == Decimal("5.00") for row in trades) / len(trades) if trades else None
    if fraction != expected["minimum_commission_fill_fraction"]:
        raise ValueError("Top20 minimum-commission diagnostic differs")
    names = {p.stem for p in directory.glob("*.csv")} | {
        p.stem for p in directory.glob("*.json") if p.stem in JSON_RECORDS}
    if set(result["records"]) != names:
        raise ValueError("Top20 canonical record inventory differs")
    proofs = {}
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            path = directory / (name + ".json")
            if rows != _read(path):
                raise ValueError("Top20 full JSON record differs: " + name)
        else:
            path = directory / (name + ".csv")
            if hashlib.sha256(pd.DataFrame(rows).to_csv(index=False).encode()).hexdigest() != _proof(path)["sha256"]:
                raise ValueError("Top20 full CSV bytes differ: " + name)
        proofs[name] = _proof(path)
    return {"passed": True, "performance_valid": result["performance_valid"],
        "comparison": "all_core_summary_keys_final_state_and_every_canonical_record_exact",
        "fixed_score_quality_reused_not_recomputed": _proof(directory / "score_quality.json"),
        "record_types": sorted(proofs), "canonical_records": proofs}


def decide_position_counts(results):
    paths = {(r["target_count"], r["period"]): r for r in results}
    if len(paths) != len(results) or set(paths) != {(n, p) for n in TARGET_COUNTS for p in PERIODS}:
        raise ValueError("All twelve predeclared paths must be present exactly once")
    base_valid = all(paths[(20, p)]["performance_valid"] is True for p in PERIODS)
    base = paths[(20, "continuous")]
    b = base["metrics"]
    qualified, comparisons = [], []
    for count in TARGET_COUNTS[1:]:
        row = paths[(count, "continuous")]
        m, risk = row["metrics"], row["diagnostics"]["risk"]
        br = base["diagnostics"]["risk"]
        valid = base_valid and all(paths[(count, p)]["performance_valid"] is True for p in PERIODS)
        checks = {"all_candidate_and_reference_paths_valid": valid}
        def compare(name, fn):
            checks[name] = bool(fn()) if valid else None
        compare("continuous_net_return_higher", lambda: Decimal(m["total_return_lower_exact"]) > Decimal(b["total_return_lower_exact"]))
        compare("continuous_drawdown_no_worse", lambda: Decimal(m["max_drawdown_conservative_exact"]) >= Decimal(b["max_drawdown_conservative_exact"]))
        compare("continuous_cost_lower", lambda: Decimal(m["total_fee_cny"]) + Decimal(m["slippage_cost_cny"]) < Decimal(b["total_fee_cny"]) + Decimal(b["slippage_cost_cny"]))
        compare("continuous_turnover_no_higher", lambda: m["one_way_turnover_average_nav"] <= b["one_way_turnover_average_nav"])
        compare("isolated_years_both_positive", lambda: all(Decimal(paths[(count, p)]["metrics"]["total_return_lower_exact"]) > 0 for p in ("2025", "2026")))
        compare("positive_month_fraction_no_lower", lambda: m["positive_month_fraction"] >= b["positive_month_fraction"])
        compare("worst_63_session_return_no_worse", lambda: m["rolling_63_sessions"]["minimum"] >= b["rolling_63_sessions"]["minimum"])
        all_risk = [paths[(count, p)]["diagnostics"]["risk"] for p in PERIODS]
        compare("concentration_coverage_complete", lambda: br["coverage_complete"] and all(r["coverage_complete"] for r in all_risk))
        complete_risk = br["coverage_complete"] and all(r["coverage_complete"] for r in all_risk)
        checks["average_exposure_reduction_at_most_5pp"] = (risk["mean_risky_nav_fraction"] >= br["mean_risky_nav_fraction"] - .05 if valid and complete_risk else None)
        checks["single_security_at_most_25pct"] = (all(r["max_single_nav_fraction"] <= .25 for r in all_risk) if valid and complete_risk else None)
        checks["top_three_at_most_65pct"] = (all(r["max_top3_nav_fraction"] <= .65 for r in all_risk) if valid and complete_risk else None)
        checks["effective_stocks_at_least_four"] = (all(r["min_effective_stock_count"] is not None and r["min_effective_stock_count"] >= 4 for r in all_risk) if valid and complete_risk else None)
        passed = all(value is True for value in checks.values())
        if passed:
            qualified.append(count)
        paired = {p: paths[(20, p)]["performance_valid"] is True and paths[(count, p)]["performance_valid"] is True for p in PERIODS}
        comparisons.append({"target_count": count, "paired_valid_periods": paired,
            "checks": checks, "qualified_for_further_prospective_evaluation": passed})
    return {"status": "INCONCLUSIVE_INVALID_REFERENCE" if not base_valid else "RETROSPECTIVE_CANDIDATES" if qualified else "NO_JOINT_IMPROVEMENT",
        "comparisons": comparisons, "qualified_counts": qualified,
        "preferred_count_for_further_review": max(qualified) if qualified else None,
        "current_paper_action": "keep_existing_top20_unchanged", "strategy_promoted": False,
        "model_retrained": False, "historical_comparison_is_new_future_OOS": False}


def _write_path(result, directory, count, diagnostics):
    directory.mkdir(parents=True)
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            _save(directory / (name + ".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
    _save(directory / "final_state.json", result["final_state"])
    _save(directory / "diagnostics.json", diagnostics)
    summary = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
    summary.update(target_count=count, diagnostics={key: value for key, value in diagnostics.items() if key != "daily_exposures"})
    _save(directory / "summary.json", summary)
    return summary


def run_position_count_comparison(declaration, output_directory, *, progress=None):
    from stopat30m.ashare_position_count_metrics import position_count_diagnostics
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve previous offline trials; choose a new directory")
    declaration = Path(declaration).resolve()
    declared = _read(declaration)
    if declared.get("kind") != KIND:
        raise ValueError("An A10 execution declaration is required")
    _check_design(declared["design"])
    bound = {**declared["bound_inputs"], "runtime_declaration": _proof(declaration)}
    _assert_proofs(bound)
    if any(bound.get(name) != proof for name, proof in implementation_proofs().items()):
        raise ValueError("Offline implementation changed after its declaration")
    source = declared["source"]
    model_before = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
    output.mkdir(parents=True)
    shutil.copyfile(declaration, output / "declaration.json")
    results, reproductions = [], {}
    _save(output / "status.json", {"phase": "loading_frozen_sources"})
    try:
        context = load_stability_context(source["snapshot"], source["scope_directory"],
            bound["protocol"]["path"], bound["market_audit"]["path"],
            source["predictions_directory"], bound["merger_actions"]["path"])
        with context["pool"].verified_batch():
            for count in TARGET_COUNTS:
                if count != 20 and set(reproductions) != set(PERIODS):
                    raise ValueError("Three full top20 reproductions must precede all reduced arms")
                for period in PERIODS:
                    status = {"phase": "ledger", "target_count": count, "period": period}
                    _save(output / "status.json", status)
                    if progress:
                        progress(status)
                    isolated = {key: value for key, value in context.items() if key != "score_coverage"}
                    result = simulate_position_count(isolated, period=period, top_k=count)
                    if count == 20:
                        reproductions[period] = assert_reference_reproduced(result,
                            Path(declared["reference_directory"]) / HOLD_RULE["key"] / period)
                    diagnostics = position_count_diagnostics(result)
                    results.append(_write_path(result, output / ("top" + str(count)) / period, count, diagnostics))
                    if progress:
                        progress({**status, "completed": True, "performance_valid": result["performance_valid"]})
        _assert_proofs(bound)
        _assert_proofs(context["bound"])
        context["pool"].assert_unchanged()
        context["actions"].assert_unchanged()
        model_after = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
        report = {"kind": "a10_fixed_lgb_position_count_results", "completed_at": datetime.now(timezone.utc).isoformat(),
            "declaration": _proof(output / "declaration.json"), "results": results,
            "baseline_full_reproduction": reproductions, "decision": decide_position_counts(results),
            "all_performance_valid": all(row["performance_valid"] is True for row in results),
            "bound_inputs": bound, "model_verification_before": model_before,
            "model_verification_after": model_after, "native_fits": 0,
            "predictions_regenerated": False, "active_paper_changed_by_experiment": False,
            "live_broker_orders": 0, "limitations": declared["design"]["interpretation"]}
        for proof in implementation_proofs().values():
            p = output / "source" / Path(proof["path"]).name
            p.parent.mkdir(exist_ok=True)
            shutil.copyfile(proof["path"], p)
        _save(output / "report.json", report)
        _save(output / "status.json", {"phase": "completed", "all_performance_valid": report["all_performance_valid"]})
        _save(output / "manifest.json", {"kind": report["kind"], "files": {
            str(p.relative_to(output)): _proof(p)["sha256"] for p in sorted(output.rglob("*")) if p.is_file()}})
        return report
    except Exception as exc:
        _save(output / "failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "completed_paths": len(results), "reproduced_periods": sorted(reproductions), "no_valid_comparison_claim": True})
        _save(output / "status.json", {"phase": "failed", "message": str(exc)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    for key in ("parent-declaration", "reference-directory", "design-path", "active-paper", "output"):
        freeze.add_argument("--" + key, type=Path, required=True)
    run = sub.add_parser("run")
    run.add_argument("--declaration", type=Path, required=True)
    run.add_argument("--output-dir", type=Path, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "freeze":
        result = freeze_position_count_declaration(**args)
        print(json.dumps({"kind": result["kind"], "declared_at": result["declared_at"]}))
    else:
        result = run_position_count_comparison(args["declaration"], args["output_dir"],
            progress=lambda row: print(json.dumps(row), flush=True))
        print(json.dumps(result["decision"]))
        if result["all_performance_valid"] is not True:
            parser.exit(1, "Invalid historical paths preserved; no qualified switch claim.\n")


if __name__ == "__main__":
    main()
