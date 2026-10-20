"""Four predeclared matched fits followed by six fee-inclusive ledger paths.

Training and execution declarations are separate. New returns cannot feed back
into the already sealed model package, original source, or research parameters.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pandas as pd

from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_horizon_protocol import (
    MODELS, EXECUTION, PERIODS, freeze_horizon_protocol, load_horizon_protocol,
)
from stopat30m.ashare_turnover_experiment import _baseline_proofs, assert_full_baseline_reproduced
from stopat30m.data.qlib_snapshot import guard_qlib_output_path


JSON_RECORDS = {"plans", "valuation_audit", "score_coverage"}


def _read(path):
    return json.loads(Path(path).read_bytes())


def execution_proofs():
    from stopat30m import ashare_horizon_validation, ashare_horizon_research
    return {**stability.implementation_proofs(),
        "horizon_execution": stability._proof(ashare_horizon_validation.__file__),
        "horizon_experiment": stability._proof(__file__),
        "horizon_training_verifier": stability._proof(ashare_horizon_research.__file__),
        "full_baseline_verifier": stability._proof(assert_full_baseline_reproduced.__code__.co_filename),
        "horizon_quality_helper": stability._proof(ashare_horizon_validation.horizon_score_quality.__globals__["_native_quality"].__code__.co_filename)}


def _verify_models(directory, protocol_path, protocol):
    from stopat30m.ashare_horizon_research import verify_horizon_predictions
    directory = Path(directory).resolve()
    bundle = _read(protocol["bound_inputs"]["source_bundle"]["path"])
    verify_horizon_predictions(directory, snapshot_path=bundle["snapshot_path"],
        scope_directory=bundle["scope_path"], protocol_path=protocol_path,
        market_audit_path=protocol["bound_inputs"]["market_audit"]["path"])
    manifest = _read(directory/"manifest.json")
    if manifest.get("kind") != "a8_shared_panel_horizon_predictions":
        raise ValueError("A complete, separately fitted common-panel model package is required")
    return manifest


def freeze_execution_declaration(protocol_path, models_directory, output):
    """Seal all four native models and the ledger code before any new returns."""
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve every previous execution declaration")
    protocol = load_horizon_protocol(protocol_path)
    models = Path(models_directory).resolve()
    manifest = _verify_models(models, protocol_path, protocol)
    baseline = Path(protocol["bound_inputs"]["original_comparison_report"]["path"]).parent
    bound = {**protocol["bound_inputs"], **execution_proofs(), **_baseline_proofs(baseline),
        "training_protocol": stability._proof(protocol_path),
        "new_model_manifest": stability._proof(models/"manifest.json")}
    for name, digest in manifest["files"].items():
        path = (models/name).resolve()
        if not path.is_relative_to(models):
            raise ValueError("New model artifact escapes its package")
        bound["new_model:"+name] = {"path": str(path), "sha256": digest}
    stability._assert_proofs(bound)
    payload = {"kind": "a8_shared_panel_horizon_execution_declaration",
        "declared_at": datetime.now(timezone.utc).isoformat(), "bound_inputs": bound,
        "models_directory": str(models), "baseline_directory": str(baseline),
        "execution": EXECUTION, "models": list(MODELS), "periods": PERIODS,
        "decision_gate": protocol["design"]["minimum_evidence_for_H20_improvement"],
        "all_four_models_verified_before_any_new_returns": True,
        "historical_windows_already_observed": True, "paper_or_broker_change_allowed": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    stability._save(output, payload)
    return payload


def decide_horizon_comparison(results):
    """One preregistered joint return, drawdown and turnover gate; no search."""
    paths = {(row["arm"], row["period"]): row for row in results}
    expected = {(arm, period) for arm in MODELS for period in PERIODS}
    if len(paths) != len(results) or set(paths) != expected:
        raise ValueError("Exactly the six predeclared model/account paths are required")
    valid = all(row["performance_valid"] is True for row in results)
    control, candidate = paths["H5", "continuous"]["metrics"], paths["H20", "continuous"]["metrics"]
    tests = {"all_six_paths_accounting_valid": valid,
        "higher_continuous_net_return_than_H5": Decimal(candidate["total_return_lower_exact"]) > Decimal(control["total_return_lower_exact"]),
        "continuous_max_drawdown_no_worse_than_H5": Decimal(candidate["max_drawdown_conservative_exact"]) >= Decimal(control["max_drawdown_conservative_exact"]),
        "continuous_turnover_no_higher_than_H5": candidate["one_way_turnover_average_nav"] <= control["one_way_turnover_average_nav"],
        "H20_positive_net_return_all_three_periods": all(Decimal(paths["H20", period]["metrics"]["total_return_lower_exact"]) > 0 for period in PERIODS)}
    qualified = all(tests.values())
    return {"predeclared_gate_results": tests,
        "H20_improves_under_declared_historical_comparison": qualified,
        "next_research": ("resolve_invalid_paths_no_model_selection" if not valid else
            "H20_candidate_requires_new_forward_evidence" if qualified else "no_H20_promotion_from_this_comparison"),
        "model_selected_for_active_paper": None, "broker_order_sent": False,
        "new_future_out_of_sample_claim": False,
        "invalid_metric_comparisons_are_diagnostic_only": not valid}


def _write_result(result, directory):
    directory.mkdir(parents=True)
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            stability._save(directory/(name+".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory/(name+".csv"), index=False)
    stability._save(directory/"final_state.json", result["final_state"])
    stability._save(directory/"score_quality.json", result["score_quality"])
    summary = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
    trades = result["records"]["trades"]
    summary["minimum_commission_fill_fraction"] = (sum(Decimal(row["commission"]) == Decimal("5")
        for row in trades)/len(trades) if trades else None)
    stability._save(directory/"summary.json", summary)
    return summary


def run_horizon_comparison(execution_declaration, output_directory, *, progress=None):
    """First reproduce all old ledgers, then evaluate exactly six sealed paths."""
    from stopat30m.ashare_horizon_validation import horizon_execution_year_scores, simulate_horizon
    declaration_path = Path(execution_declaration).resolve()
    declaration_bytes = declaration_path.read_bytes()
    declaration_proof = {"path": str(declaration_path),
        "sha256": hashlib.sha256(declaration_bytes).hexdigest()}
    declaration = json.loads(declaration_bytes)
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve every prior model comparison, including invalid paths")
    if (declaration.get("kind") != "a8_shared_panel_horizon_execution_declaration"
            or declaration.get("execution") != EXECUTION or declaration.get("models") != list(MODELS)
            or declaration.get("periods") != PERIODS
            or declaration.get("all_four_models_verified_before_any_new_returns") is not True
            or declaration.get("paper_or_broker_change_allowed") is not False):
        raise ValueError("An unchanged sealed execution declaration is required")
    bound = {**declaration["bound_inputs"], "runtime_declaration": declaration_proof}
    stability._assert_proofs(bound)
    if any(bound.get(key) != proof for key, proof in execution_proofs().items()):
        raise ValueError("Execution implementation changed after the runtime freeze")
    protocol_path = bound["training_protocol"]["path"]
    protocol = load_horizon_protocol(protocol_path)
    if declaration["decision_gate"] != protocol["design"]["minimum_evidence_for_H20_improvement"]:
        raise ValueError("Research decision criteria changed after declaration")
    models_directory = Path(declaration["models_directory"])
    _verify_models(models_directory, protocol_path, protocol)
    _baseline_proofs(declaration["baseline_directory"])
    bundle = _read(bound["source_bundle"]["path"])
    source_archive = {}
    for key, proof in execution_proofs().items():
        source_archive[key] = Path(proof["path"]).read_bytes()
        if hashlib.sha256(source_archive[key]).hexdigest() != proof["sha256"]:
            raise ValueError("Execution source changed before archival")
    output.mkdir(parents=True)
    (output/"declaration.json").write_bytes(declaration_bytes)
    archive = output/"source"
    archive.mkdir()
    for key, payload in source_archive.items():
        (archive/(key+"_"+Path(bound[key]["path"]).name)).write_bytes(payload)
    results, reproductions = [], {}
    try:
        stability._save(output/"status.json", {"phase": "load_verified_original_sources"})
        context = stability.load_stability_context(bundle["snapshot_path"], bundle["scope_path"],
            bound["original_protocol"]["path"], bound["market_audit"]["path"],
            Path(bound["original_model_manifest"]["path"]).parent, bound["merger_actions"]["path"])
        predictions = pd.read_csv(models_directory/"predictions.csv")
        scores = {arm: horizon_execution_year_scores(predictions, context["calendar"], arm=arm) for arm in MODELS}
        if not scores["H5"].index.equals(scores["H20"].index):
            raise ValueError("The two model arms do not cover the same inference stock/date panel")
        with context["pool"].verified_batch():
            for period in PERIODS:
                status = {"phase": "original_baseline_reproduction", "period": period}
                stability._save(output/"status.json", status)
                if progress:
                    progress(status)
                isolated = {key: value for key, value in context.items() if key != "score_coverage"}
                result = stability.simulate_stability(isolated, period=period, model="LGB")
                result["score_quality"] = stability.score_quality(isolated, period=period, model="LGB")
                reproductions[period] = assert_full_baseline_reproduced(result,
                    Path(declaration["baseline_directory"])/period/"LGB")
            stability._save(output/"original_baseline_reproduction.json", reproductions)
            if len(reproductions) != 3 or not all(row["passed"] for row in reproductions.values()):
                raise ValueError("All three exact baseline reproductions must precede new returns")
            del predictions
            for arm in MODELS:
                for period in PERIODS:
                    status = {"phase": "new_costed_ledger", "arm": arm, "period": period}
                    stability._save(output/"status.json", status)
                    if progress:
                        progress(status)
                    result = simulate_horizon(context, period=period, arm=arm, scores=scores[arm])
                    summary = _write_result(result, output/arm/period)
                    results.append(summary)
                    if progress:
                        progress({**status, "completed": True, "valid": summary["performance_valid"],
                            "net_return": summary["metrics"]["total_return_lower_exact"],
                            "max_drawdown": summary["metrics"]["max_drawdown_conservative_exact"],
                            "turnover": summary["metrics"]["one_way_turnover_average_nav"]})
        stability._assert_proofs(bound)
        stability._assert_proofs(context["bound"])
        context["pool"].assert_unchanged()
        context["actions"].assert_unchanged()
        _verify_models(models_directory, protocol_path, protocol)
        report = {"kind": "a8_shared_panel_horizon_costed_comparison",
            "completed_at": datetime.now(timezone.utc).isoformat(), "results": results,
            "training_protocol": stability._proof(protocol_path), "bound_inputs": bound,
            "original_baseline_reproduction": reproductions,
            "all_performance_valid": all(row["performance_valid"] is True for row in results),
            "decision": decide_horizon_comparison(results), "models": list(MODELS),
            "execution": EXECUTION, "validity": protocol["validity"],
            "new_fit_count": 4, "new_ledger_count": 6,
            "H5_is_a_new_matched_control_not_the_old_frozen_H5": True,
            "inference_panels_equal": True,
            "score_quality_is_each_arms_native_maturity_not_matched_IC_comparison": True,
            "tax_classification_unresolved": context["market_audit"].get("tax_classification_unresolved", []),
            "active_paper_changed": False, "broker_order_sent": False,
            "limitations": ["Previously observed historical windows, not fresh future out-of-sample evidence.",
                "New H5 control shares H20's21-session mature training panel; it cannot be equated to the old H5 model or A6 execution.",
                "Native label quality has different maturity tails; only the common fee-ledger comparison is the primary model control.",
                "Unpriced delisted rights invalidate a path; no proxy value, cutoff change, cash recovery or security exclusion overrides that gate.",
                "Daily observed quotes do not establish queue priority, broker fills or liquidation values.",
                "Maximum conservative distribution-tax reserves and unresolved source classifications remain disclosed.",
                "No model is promoted to an active paper or real account."]}
        stability._save(output/"report.json", report)
        stability._save(output/"status.json", {"phase": "completed", "all_performance_valid": report["all_performance_valid"]})
        stability._save(output/"manifest.json", {"kind": report["kind"], "bound_inputs": bound,
            "files": {str(path.relative_to(output)): stability.sha(path) for path in sorted(output.rglob("*")) if path.is_file()}})
        return report
    except Exception as exc:
        stability._save(output/"failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "completed_new_paths": len(results), "reproduced_baselines": sorted(reproductions),
            "no_qualified_comparison_claim": True})
        stability._save(output/"status.json", {"phase": "failed", "message": str(exc)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze-training")
    freeze.add_argument("--design-freeze", type=Path, required=True)
    freeze.add_argument("--output", type=Path, required=True)
    train = commands.add_parser("train")
    train.add_argument("--protocol", type=Path, required=True)
    train.add_argument("--output-dir", type=Path, required=True)
    seal = commands.add_parser("freeze-execution")
    seal.add_argument("--protocol", type=Path, required=True)
    seal.add_argument("--models-dir", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("validate")
    run.add_argument("--declaration", type=Path, required=True)
    run.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    progress = lambda row: print(json.dumps(row, ensure_ascii=False), flush=True)
    if args.command == "freeze-training":
        result = freeze_horizon_protocol(args.design_freeze, args.output)
        print(json.dumps({"declared_at": result["declared_at"], "fits_budget": 4}))
    elif args.command == "train":
        from stopat30m.ashare_horizon_research import build_horizon_predictions
        load_horizon_protocol(args.protocol)
        build_horizon_predictions(args.protocol, args.output_dir, progress=progress)
    elif args.command == "freeze-execution":
        result = freeze_execution_declaration(args.protocol, args.models_dir, args.output)
        print(json.dumps({"declared_at": result["declared_at"], "ledger_budget": 6}))
    elif args.command == "validate":
        result = run_horizon_comparison(args.declaration, args.output_dir, progress=progress)
        print(json.dumps(result["decision"], ensure_ascii=False))
        if result["all_performance_valid"] is not True:
            parser.exit(1, "Invalid paths retained; do not select a model from indicative returns.\n")


if __name__ == "__main__":
    main()
