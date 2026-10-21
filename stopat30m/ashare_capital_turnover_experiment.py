"""Predeclared offline capital turnover comparison; no live paper execution."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd

from stopat30m.ashare_capital_turnover_validation import CAPITAL_ARMS, HOLD_RULE, simulate_capital_turnover
from stopat30m.ashare_position_count_experiment import assert_reference_reproduced
from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.ashare_stability_validation import _assert_proofs, _proof, _save, load_stability_context
from stopat30m.ashare_turnover_experiment import JSON_RECORDS as ORIGINAL_JSON_RECORDS, PERIODS
from stopat30m.data.qlib_snapshot import guard_qlib_output_path

KIND = "a11_fixed_lgb_capital_turnover_declaration"
JSON_RECORDS = (*ORIGINAL_JSON_RECORDS, "capital_turnover")
DESIGN_CANONICAL_SHA256 = "30a2d16dab189703fd8667508129467b92a745873134f5b8df6e85817e2f8d1a"


def _read(path):
    return json.loads(Path(path).read_bytes())


def _check_design(design):
    digest = hashlib.sha256(json.dumps(design, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if digest != DESIGN_CANONICAL_SHA256:
        raise ValueError("The entire predeclared A11 design must remain unchanged")


def implementation_proofs():
    from stopat30m import ashare_capital_turnover_validation, ashare_capital_turnover_execution
    return {key: _proof(path) for key, path in {
        "a11_experiment": __file__, "a11_validation": ashare_capital_turnover_validation.__file__,
        "a11_execution": ashare_capital_turnover_execution.__file__,
    }.items()}


def _check_output_isolation(output, paper):
    output, paper = Path(output).resolve(), Path(paper).resolve()
    if output.is_relative_to(paper) or paper.is_relative_to(output):
        raise ValueError("Offline output must remain separate from the active paper directory")


def _check_inheritance(declared):
    parent = _read(declared["bound_inputs"]["a11_parent"]["path"])
    if parent.get("kind") != "a10_fixed_lgb_position_count_declaration":
        raise ValueError("The frozen A10 parent is required")
    if any(declared[k] != parent[k] for k in ("source", "reference_directory", "active_paper")):
        raise ValueError("Inherited frozen source, reference and paper paths must match the parent")
    if any(declared["bound_inputs"].get(k) != p for k, p in parent["bound_inputs"].items()):
        raise ValueError("Every inherited parent proof must remain unchanged")


def freeze_capital_turnover_declaration(*, parent_declaration, design_path, output):
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve frozen declarations; choose a new output path")
    parent_path, design_path = Path(parent_declaration).resolve(), Path(design_path).resolve()
    parent, design = _read(parent_path), _read(design_path)
    _check_design(design)
    if parent.get("kind") != "a10_fixed_lgb_position_count_declaration":
        raise ValueError("The original A10 source declaration is required")
    _check_output_isolation(output, parent["active_paper"])
    bound = {**parent["bound_inputs"], **implementation_proofs(),
             "a11_parent": _proof(parent_path), "a11_design": _proof(design_path)}
    _assert_proofs(bound)
    source = parent["source"]
    verified = verify_reused_stability_models(source["predictions_directory"],
        bound["protocol"]["path"], bound["market_audit"]["path"])
    paper = Path(parent["active_paper"])
    manifest = _read(paper / "manifest.json")
    if manifest.get("kind") != "isolated_ashare_low_turnover_forward" or manifest["strategy"]["top_k"] != 20:
        raise ValueError("Only the existing unchanged top20 paper may be observed")
    payload = {"kind": KIND, "declared_at": datetime.now(timezone.utc).isoformat(),
        "design": design, "bound_inputs": bound, "source": source,
        "reference_directory": parent["reference_directory"], "model_verification": verified,
        "active_paper": str(paper), "active_state_at_declaration": _proof(paper / "state.json"),
        "active_state_is_observation_not_frozen_input": True,
        "fits_performed": 0, "predictions_regenerated": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    _save(output, payload)
    return payload


def _period_checks(row, base):
    if row["performance_valid"] is not True or base["performance_valid"] is not True:
        return None
    m, b = row["metrics"], base["metrics"]
    cost = lambda x: Decimal(x["total_fee_cny"]) + Decimal(x["slippage_cost_cny"])
    return {"net_positive": Decimal(m["total_return_lower_exact"]) > 0,
        "net_no_worse": Decimal(m["total_return_lower_exact"]) >= Decimal(b["total_return_lower_exact"]),
        "drawdown_no_worse": Decimal(m["max_drawdown_conservative_exact"]) >= Decimal(b["max_drawdown_conservative_exact"]),
        "cost_lower": cost(m) < cost(b),
        "turnover_lower": m["one_way_turnover_average_nav"] < b["one_way_turnover_average_nav"],
        "exposure_reduction_at_most_5pp": m["mean_invested_fraction"] >= b["mean_invested_fraction"] - .05}


def decide_capital_turnover(results):
    fractions = dict(CAPITAL_ARMS)
    if any(r.get("arm_key") not in fractions or r.get("fraction") != fractions[r["arm_key"]]
           or r.get("fraction") is not None and type(r["fraction"]) is not str for r in results):
        raise ValueError("Each arm must retain its exact declared fraction")
    paths = {(r["arm_key"], r["period"]): r for r in results}
    if len(paths) != len(results) or set(paths) != {(a, p) for a, _ in CAPITAL_ARMS for p in PERIODS}:
        raise ValueError("All twelve predeclared paths are required exactly once")
    base_valid = all(paths[(HOLD_RULE["key"], p)]["performance_valid"] is True for p in PERIODS)
    b = paths[(HOLD_RULE["key"], "continuous")]["metrics"]
    qualified, comparisons = [], []
    for key, fraction in CAPITAL_ARMS[1:]:
        row, all_rows = paths[(key, "continuous")], [paths[(key, p)] for p in PERIODS]
        m = row["metrics"]
        valid = base_valid and all(r["performance_valid"] is True for r in all_rows)
        paired = {p: _period_checks(paths[(key, p)], paths[(HOLD_RULE["key"], p)]) for p in PERIODS}
        checks = {"all_paths_valid": valid}
        def compare(name, fn):
            checks[name] = bool(fn()) if valid else None
        compare("continuous_net_higher", lambda: Decimal(m["total_return_lower_exact"]) > Decimal(b["total_return_lower_exact"]))
        compare("continuous_drawdown_no_worse", lambda: Decimal(m["max_drawdown_conservative_exact"]) >= Decimal(b["max_drawdown_conservative_exact"]))
        compare("continuous_cost_lower", lambda: Decimal(m["total_fee_cny"]) + Decimal(m["slippage_cost_cny"]) < Decimal(b["total_fee_cny"]) + Decimal(b["slippage_cost_cny"]))
        compare("continuous_turnover_lower", lambda: m["one_way_turnover_average_nav"] < b["one_way_turnover_average_nav"])
        compare("isolated_periods_joint_improvement", lambda: all(all(paired[p].values()) for p in ("2025", "2026")))
        compare("continuous_exposure_no_reduction_over_5pp", lambda: m["mean_invested_fraction"] >= b["mean_invested_fraction"] - .05)
        compare("positive_month_fraction_no_lower", lambda: m["positive_month_fraction"] >= b["positive_month_fraction"])
        compare("worst63_no_worse", lambda: m["rolling_63_sessions"]["minimum"] >= b["rolling_63_sessions"]["minimum"])
        risks = [r["diagnostics"]["risk"] for r in all_rows]
        coverage = all(r["coverage_complete"] for r in risks)
        compare("concentration_coverage_complete", lambda: coverage)
        checks["concentration_limits_pass"] = (all(r["max_single_nav_fraction"] <= .25 and r["max_top3_nav_fraction"] <= .65
            and r["min_effective_stock_count"] is not None and r["min_effective_stock_count"] >= 4 for r in risks)
            if valid and coverage else None)
        passed = all(v is True for v in checks.values())
        if passed:
            qualified.append(key)
        comparisons.append({"arm_key": key, "fraction": fraction, "formal_checks": checks,
            "paired_valid_period_diagnostics": paired, "qualified_for_further_prospective_evaluation": passed})
    return {"status": "INCONCLUSIVE_INVALID_REFERENCE" if not base_valid else "RETROSPECTIVE_CANDIDATES" if qualified else "NO_JOINT_IMPROVEMENT",
        "comparisons": comparisons, "qualified_arms": qualified,
        "preferred_arm_for_further_review": qualified[-1] if qualified else None,
        "current_paper_action": "keep_existing_top20_unchanged", "strategy_promoted": False,
        "model_retrained": False, "historical_comparison_is_new_future_OOS": False}


def _write_path(result, directory, arm_key, fraction, diagnostics):
    directory.mkdir(parents=True)
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            _save(directory / (name + ".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
    _save(directory / "final_state.json", result["final_state"])
    _save(directory / "diagnostics.json", diagnostics)
    summary = {k: v for k, v in result.items() if k not in {"records", "final_state"}}
    summary.update(arm_key=arm_key, fraction=fraction, diagnostics={k: v for k, v in diagnostics.items() if k != "daily_exposures"})
    _save(directory / "summary.json", summary)
    return summary


def run_capital_turnover_comparison(declaration, output_directory, *, progress=None):
    from stopat30m.ashare_position_count_metrics import position_count_diagnostics
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve previous offline trials; choose a new directory")
    declaration = Path(declaration).resolve()
    declared = _read(declaration)
    if declared.get("kind") != KIND:
        raise ValueError("An A11 execution declaration is required")
    _check_design(declared["design"])
    bound = {**declared["bound_inputs"], "runtime_declaration": _proof(declaration)}
    _assert_proofs(bound)
    _check_inheritance(declared)
    _check_output_isolation(output, declared["active_paper"])
    if any(bound.get(k) != p for k, p in implementation_proofs().items()):
        raise ValueError("Capital turnover implementation changed after declaration")
    source = declared["source"]
    model_before = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
    output.mkdir(parents=True)
    shutil.copyfile(declaration, output / "declaration.json")
    results, reproductions = [], {}
    _save(output / "status.json", {"phase": "loading_frozen_sources"})
    try:
        context = load_stability_context(source["snapshot"], source["scope_directory"],
            bound["protocol"]["path"], bound["market_audit"]["path"], source["predictions_directory"], bound["merger_actions"]["path"])
        with context["pool"].verified_batch():
            for key, fraction in CAPITAL_ARMS:
                if fraction is not None and set(reproductions) != set(PERIODS):
                    raise ValueError("Three exact baseline reproductions must precede candidates")
                for period in PERIODS:
                    status = {"phase": "ledger", "arm_key": key, "fraction": fraction, "period": period}
                    _save(output / "status.json", status)
                    if progress:
                        progress(status)
                    isolated = {k: v for k, v in context.items() if k != "score_coverage"}
                    result = simulate_capital_turnover(isolated, period=period, fraction=fraction)
                    if fraction is None:
                        reproductions[period] = assert_reference_reproduced(result, Path(declared["reference_directory"]) / HOLD_RULE["key"] / period)
                    diagnostics = position_count_diagnostics(result)
                    if fraction is not None:
                        guards = result["records"]["capital_turnover"]
                        if len(guards) != len(result["records"]["turnover_plans"]):
                            raise ValueError("Every candidate rebalance must retain its capital guard audit")
                    results.append(_write_path(result, output / key / period, key, fraction, diagnostics))
                    if progress:
                        progress({**status, "completed": True, "performance_valid": result["performance_valid"]})
        _assert_proofs(bound)
        _assert_proofs(context["bound"])
        context["pool"].assert_unchanged()
        context["actions"].assert_unchanged()
        model_after = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
        report = {"kind": "a11_fixed_lgb_capital_turnover_results", "completed_at": datetime.now(timezone.utc).isoformat(),
            "declaration": _proof(output / "declaration.json"), "results": results,
            "baseline_full_reproduction": reproductions, "decision": decide_capital_turnover(results),
            "all_performance_valid": all(r["performance_valid"] is True for r in results),
            "bound_inputs": bound, "model_verification_before": model_before, "model_verification_after": model_after,
            "native_fits": 0, "predictions_regenerated": False, "active_paper_changed_by_experiment": False,
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
    for key in ("parent-declaration", "design-path", "output"):
        freeze.add_argument("--" + key, type=Path, required=True)
    run = sub.add_parser("run")
    run.add_argument("--declaration", type=Path, required=True)
    run.add_argument("--output-dir", type=Path, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "freeze":
        result = freeze_capital_turnover_declaration(**args)
        print(json.dumps({"kind": result["kind"], "declared_at": result["declared_at"]}))
    else:
        result = run_capital_turnover_comparison(args["declaration"], args["output_dir"],
            progress=lambda row: print(json.dumps(row), flush=True))
        print(json.dumps(result["decision"]))
        if result["all_performance_valid"] is not True:
            parser.exit(1, "Invalid historical paths preserved; no qualified switch claim.\n")


if __name__ == "__main__":
    main()
