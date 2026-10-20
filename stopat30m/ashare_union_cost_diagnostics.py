"""Declared cost sensitivities of an already completed fixed experiment.

These optimistic counterfactuals diagnose costs; they are not broker offers,
new out-of-sample evidence, model selection, or a replacement paper account.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd

from stopat30m.ashare_union_validation import (
    MODELS, _assert_proofs, _proof, _save, load_union_context, simulate_union,
)
from stopat30m.data.qlib_snapshot import guard_qlib_output_path


SCENARIOS = {
    "declared_costs": {},
    "no_minimum_commission_floor": {"minimum_commission_cny": "0"},
    "zero_adverse_slippage": {"adverse_slippage_bps": "0"},
    "no_floor_zero_adverse_slippage": {
        "minimum_commission_cny": "0", "adverse_slippage_bps": "0"},
}


def scenario_context(context, name):
    """Isolate settings/caches while sharing read-only verified market arrays."""
    if name not in SCENARIOS:
        raise ValueError("Undeclared cost sensitivity")
    result = {key: value for key, value in context.items() if key != "score_coverage"}
    result["protocol"] = deepcopy(context["protocol"])
    result["protocol"]["execution"].update(SCENARIOS[name])
    return result


def assert_baseline_reproduced(result, summary, final_state):
    if (result["metrics"] != summary["metrics"]
            or result["performance_valid"] != summary["performance_valid"]
            or result["final_state"] != final_state):
        raise ValueError("Declared baseline does not reproduce; stop cost sensitivities")


def run_cost_diagnostics(*, baseline_directory, output_directory, snapshot,
                         scope_directory, protocol, market_audit, predictions,
                         merger_actions, progress=None):
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve every previous cost diagnostic")
    baseline = Path(baseline_directory).resolve()
    manifest_path = baseline / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("kind") != "a5_union_costed_comparison":
        raise ValueError("A completed fixed union comparison is required")
    baseline_proofs = {}
    for name, digest in manifest["files"].items():
        path = (baseline / name).resolve()
        if not path.is_relative_to(baseline):
            raise ValueError("Baseline artifact escapes its directory")
        baseline_proofs["baseline:" + name] = {"path": str(path), "sha256": digest}
    _assert_proofs(baseline_proofs)
    context = load_union_context(snapshot, scope_directory, protocol, market_audit,
                                 predictions, merger_actions)
    report = json.loads((baseline / "report.json").read_text())
    if (report["bound_inputs"]["protocol"]["sha256"] != _proof(protocol)["sha256"]
            or report["bound_inputs"]["predictions_manifest"]["sha256"]
            != _proof(Path(predictions) / "manifest.json")["sha256"]):
        raise ValueError("Cost diagnostics must use the original frozen model generation")
    bound = {**baseline_proofs, **context["bound"],
             "baseline_manifest": _proof(manifest_path), "diagnostic_implementation": _proof(__file__)}
    declaration = {
        "kind": "a5_fixed_cost_sensitivity_declaration",
        "declared_at": datetime.now(timezone.utc).isoformat(),
        "bound_inputs": bound, "scenarios": SCENARIOS, "period": "continuous",
        "models": list(MODELS), "fitting_or_parameter_search": False,
        "selection_or_active_paper_change": False,
        "interpretation": "Optimistic execution-cost counterfactuals with sizing/cash feedback, not additive attribution or available broker terms.",
    }
    output.mkdir(parents=True)
    _save(output / "declaration.json", declaration)
    results = []
    with context["pool"].verified_batch():
        for model in MODELS:
            for scenario in SCENARIOS:
                if progress:
                    progress({"model": model, "scenario": scenario})
                result = simulate_union(scenario_context(context, scenario),
                                        period="continuous", model=model)
                if scenario == "declared_costs":
                    summary = json.loads((baseline / "continuous" / model / "summary.json").read_text())
                    final = json.loads((baseline / "continuous" / model / "final_state.json").read_text())
                    assert_baseline_reproduced(result, summary, final)
                directory = output / model / scenario
                directory.mkdir(parents=True)
                for name in ("curves", "trades", "orders", "corporate_actions"):
                    pd.DataFrame(result["records"][name]).to_csv(directory / (name + ".csv"), index=False)
                _save(directory / "final_state.json", result["final_state"])
                summary = {key: value for key, value in result.items()
                           if key not in {"records", "final_state"}}
                summary["scenario"] = scenario
                _save(directory / "summary.json", summary)
                results.append(summary)
    _assert_proofs(bound)
    context["pool"].assert_unchanged()
    context["actions"].assert_unchanged()
    payload = {"kind": "a5_fixed_cost_sensitivity_results", "declaration": _proof(output / "declaration.json"),
               "results": results, "all_performance_valid": all(r["performance_valid"] for r in results),
               "selection_performed": False, "active_paper_changed": False}
    _save(output / "report.json", payload)
    _save(output / "manifest.json", {"bound_inputs": bound, "files": {
        str(path.relative_to(output)): _proof(path)["sha256"]
        for path in sorted(output.rglob("*")) if path.is_file()}})
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline-directory", "output-directory", "snapshot", "scope-directory",
                 "protocol", "market-audit", "predictions", "merger-actions"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = vars(parser.parse_args())
    report = run_cost_diagnostics(**args, progress=lambda row: print(json.dumps(row), flush=True))
    for row in report["results"]:
        print(json.dumps({"model": row["model"], "scenario": row["scenario"],
            "return": row["metrics"]["total_return_lower_exact"],
            "drawdown": row["metrics"]["max_drawdown_conservative_exact"],
            "performance_valid": row["performance_valid"]}), flush=True)


if __name__ == "__main__":
    main()
