"""Frozen lagged exposure matching with actual core and cash-control costs."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd

from stopat30m.ashare_exposure_matching_validation import MATCHING_ARMS, HOLD_RULE, simulate_exposure_matching
from stopat30m.ashare_capital_turnover_experiment import JSON_RECORDS as A11_JSON_RECORDS
from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.ashare_stability_validation import _assert_proofs, _proof, _save, load_stability_context
from stopat30m.ashare_turnover_experiment import JSON_RECORDS as ORIGINAL_JSON_RECORDS, PERIODS
from stopat30m.data.qlib_snapshot import guard_qlib_output_path

KIND = "a12_lagged_exposure_matching_declaration"
JSON_RECORDS = (*A11_JSON_RECORDS, "exposure_matching")
DESIGN_CANONICAL_SHA256 = "3d2add8402ff40633b47d7ccf28bb950b4480c4e5d7394716ef8445b1cf94730"


def _read(path):
    return json.loads(Path(path).read_bytes())


def _check_design(design):
    digest = hashlib.sha256(json.dumps(design, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if digest != DESIGN_CANONICAL_SHA256:
        raise ValueError("The entire predeclared A12 design must remain unchanged")


def implementation_proofs():
    from stopat30m import ashare_exposure_matching_validation, ashare_exposure_matching_execution
    return {key: _proof(path) for key, path in {
        "a12_experiment": __file__, "a12_validation": ashare_exposure_matching_validation.__file__,
        "a12_execution": ashare_exposure_matching_execution.__file__,
    }.items()}


def _check_output_isolation(output, paper):
    output, paper = Path(output).resolve(), Path(paper).resolve()
    if output.is_relative_to(paper) or paper.is_relative_to(output):
        raise ValueError("Offline output must remain separate from the active paper directory")


def _check_inheritance(declared):
    parent = _read(declared["bound_inputs"]["a12_parent"]["path"])
    if parent.get("kind") != "a11_fixed_lgb_capital_turnover_declaration":
        raise ValueError("The frozen A11 parent is required")
    if any(declared[k] != parent[k] for k in ("source", "active_paper")):
        raise ValueError("Inherited source and active paper must match parent")
    if any(declared["bound_inputs"].get(k) != p for k, p in parent["bound_inputs"].items()):
        raise ValueError("Every inherited source proof must remain unchanged")
    reference = Path(declared["reference_directory"]).resolve()
    if (Path(declared["bound_inputs"]["a12_reference_report"]["path"]).parent != reference
            or _proof(reference / "declaration.json")["sha256"] != declared["bound_inputs"]["a12_parent"]["sha256"]):
        raise ValueError("Reference ledgers must be the exact frozen parent execution")


def freeze_exposure_matching_declaration(*, parent_declaration, reference_directory, design_path, output):
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve prior declarations; choose a new output path")
    parent_path, design_path = Path(parent_declaration).resolve(), Path(design_path).resolve()
    parent, design = _read(parent_path), _read(design_path)
    _check_design(design)
    if parent.get("kind") != "a11_fixed_lgb_capital_turnover_declaration":
        raise ValueError("The frozen A11 source declaration is required")
    _check_output_isolation(output, parent["active_paper"])
    reference = Path(reference_directory).resolve()
    inventory = _read(reference / "manifest.json")
    if inventory.get("kind") != "a11_fixed_lgb_capital_turnover_results":
        raise ValueError("The complete original A11 reference is required")
    if _proof(reference / "declaration.json")["sha256"] != _proof(parent_path)["sha256"]:
        raise ValueError("Reference declaration differs from parent")
    bound = {**parent["bound_inputs"], **implementation_proofs(),
        "a12_parent": _proof(parent_path), "a12_design": _proof(design_path),
        "a12_reference_report": _proof(reference / "report.json"),
        "a12_reference_manifest": _proof(reference / "manifest.json")}
    for name, digest in inventory["files"].items():
        path = (reference / name).resolve()
        if not path.is_relative_to(reference):
            raise ValueError("Reference artifact escapes its directory")
        bound["a12_reference:" + name] = {"path": str(path), "sha256": digest}
    _assert_proofs(bound)
    source = parent["source"]
    verified = verify_reused_stability_models(source["predictions_directory"],
        bound["protocol"]["path"], bound["market_audit"]["path"])
    paper = Path(parent["active_paper"])
    active = _read(paper / "manifest.json")
    if active.get("kind") != "isolated_ashare_low_turnover_forward" or active["strategy"]["top_k"] != 20:
        raise ValueError("The active paper must retain its existing top20 configuration")
    payload = {"kind": KIND, "declared_at": datetime.now(timezone.utc).isoformat(),
        "design": design, "bound_inputs": bound, "source": source,
        "reference_directory": str(reference), "model_verification": verified,
        "active_paper": str(paper), "active_state_at_declaration": _proof(paper / "state.json"),
        "active_state_is_observation_not_frozen_input": True,
        "fits_performed": 0, "predictions_regenerated": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    _save(output, payload)
    return payload


def assert_matching_reference_reproduced(result, directory):
    directory = Path(directory)
    expected = _read(directory / "summary.json")
    expected = {k: v for k, v in expected.items() if k not in {"arm_key", "fraction", "diagnostics"}}
    actual = {k: v for k, v in result.items() if k not in {"records", "final_state"}}
    if actual != expected or result["final_state"] != _read(directory / "final_state.json"):
        raise ValueError("Full original core summary or final state differs")
    names = {p.stem for p in directory.glob("*.csv")} | {
        p.stem for p in directory.glob("*.json") if p.stem in A11_JSON_RECORDS}
    if set(result["records"]) != names:
        raise ValueError("Original canonical record inventory differs")
    proofs = {}
    for name, rows in result["records"].items():
        path = directory / (name + (".json" if name in A11_JSON_RECORDS else ".csv"))
        if name in A11_JSON_RECORDS:
            same = rows == _read(path)
        else:
            same = hashlib.sha256(pd.DataFrame(rows).to_csv(index=False).encode()).hexdigest() == _proof(path)["sha256"]
        if not same:
            raise ValueError("Full original record differs: " + name)
        proofs[name] = _proof(path)
    return {"passed": True, "performance_valid": result["performance_valid"],
        "record_types": sorted(proofs), "canonical_records": proofs}


def exposure_match_diagnostics(reference_diagnostics, matched_diagnostics, *, reference_valid, matched_valid):
    import math
    import numpy as np
    reference, matched = reference_diagnostics["daily_exposures"], matched_diagnostics["daily_exposures"]
    dates = [r["date"] for r in reference]
    matched_dates = [r["date"] for r in matched]
    if (not dates or dates != sorted(set(dates)) or matched_dates != dates
            and not (matched_valid is False and matched_dates == dates[:len(matched_dates)])):
        raise ValueError("Matching requires every same actual session; never trim or fill dates")
    matched_by_day = {row["date"]: row for row in matched}
    values, unknown, unknown_reasons, noncash_gaps, over, longest, run = [], [], [], [], [], 0, 0
    for a in reference:
        b = matched_by_day.get(a["date"])
        if b is not None and "cash_nav_fraction" in a and "cash_nav_fraction" in b:
            left_cash, right_cash = a["cash_nav_fraction"], b["cash_nav_fraction"]
            if (type(left_cash) not in (int, float) or type(right_cash) not in (int, float)
                    or not math.isfinite(left_cash) or not math.isfinite(right_cash)):
                raise ValueError("Accounting cash fractions must be finite when provided")
            noncash_gaps.append({"date": a["date"], "reference": 1-left_cash,
                "matched": 1-right_cash, "signed_gap": left_cash-right_cash})
        if b is None or a.get("coverage_complete") is not True or b.get("coverage_complete") is not True:
            unknown.append(a["date"]); run = 0
            unknown_reasons.append({"date": a["date"], "matched_close_present": b is not None,
                "reference_coverage_gaps": a.get("coverage_gaps", []),
                "matched_coverage_gaps": b.get("coverage_gaps", []) if b is not None else ["no_recorded_close_after_stopped_path"]})
            continue
        left, right = a.get("risky_nav_fraction"), b.get("risky_nav_fraction")
        if (type(left) not in (int, float) or type(right) not in (int, float)
                or not math.isfinite(left) or not math.isfinite(right) or left < 0 or right < 0):
            raise ValueError("Known matching exposure needs finite nonnegative security values")
        gap = Decimal(str(right)) - Decimal(str(left))
        values.append({"date": a["date"], "reference": left, "matched": right, "signed_gap": float(gap), "signed_gap_exact": str(gap)})
        if abs(gap) > Decimal(".05"):
            over.append(a["date"]); run += 1; longest = max(longest, run)
        else:
            run = 0
    exact_gaps = [Decimal(v["signed_gap_exact"]) for v in values]
    gaps = [float(v) for v in exact_gaps]
    coverage = len(values) / len(dates)
    mean = sum(exact_gaps, Decimal(0)) / len(gaps) if gaps else None
    mae = sum(map(abs, exact_gaps), Decimal(0)) / len(gaps) if gaps else None
    absolute = sorted(map(abs, exact_gaps))
    location = Decimal(".95") * (len(absolute)-1) if absolute else None
    low = int(location) if absolute else 0
    p95 = (absolute[low] + (absolute[min(low+1, len(absolute)-1)] - absolute[low]) * (location-low)) if absolute else None
    metrics = {"mean_signed_gap": float(mean) if gaps else None,
        "mean_absolute_gap": float(mae) if gaps else None,
        "p95_absolute_gap": float(p95) if gaps else None,
        "gap_RMSE": float(np.sqrt(np.mean(np.square(gaps)))) if gaps else None,
        "max_absolute_gap": max(map(abs, gaps)) if gaps else None}
    checks = {"both_paths_valid": reference_valid is True and matched_valid is True,
        "known_exposure_coverage_at_least_99pct": coverage >= .99,
        "abs_mean_signed_gap_at_most_1pp": abs(mean) <= Decimal(".01") if gaps else None,
        "mean_absolute_gap_at_most_2pp": mae <= Decimal(".02") if gaps else None,
        "p95_absolute_gap_at_most_5pp": p95 <= Decimal(".05") if gaps else None}
    passed = all(v is True for v in checks.values())
    status = "MATCHED" if passed else "INVALID_PATH" if not checks["both_paths_valid"] else "UNKNOWN_EXPOSURE_COVERAGE" if coverage < .99 else "MATCHING_FAILED"
    return {"status": status, "passed": passed, "checks": checks, **metrics,
        "session_count": len(dates), "known_exposure_days": len(values), "known_exposure_coverage": coverage,
        "unknown_dates": unknown, "unknown_reasons": unknown_reasons,
        "above_5pp_dates": over, "longest_known_above_5pp_run": longest,
        "accounting_noncash_gap": {"definition": "1-cash/lower_NAV_including_recorded_noncash_assets_not_a_security_risk_substitute",
            "recorded_pair_count": len(noncash_gaps),
            "mean_signed_gap": float(np.mean([v["signed_gap"] for v in noncash_gaps])) if noncash_gaps else None,
            "mean_absolute_gap": float(np.mean([abs(v["signed_gap"]) for v in noncash_gaps])) if noncash_gaps else None,
            "daily_gaps": noncash_gaps, "used_for_matching_gate": False},
        "daily_gaps": values, "threshold_arithmetic": "Decimal_of_serialized_exposure_linear_p95_inclusive_bounds", "unknown_is_not_zero_or_omitted_NAV": True,
        "matching_does_not_equal_beta_sector_or_volatility_matching": True}


def purpose_costs(result, arm_key):
    fields = ("notional", "commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")
    costs = {purpose: {"fill_count": 0, **{k + "_cny": Decimal(0) for k in fields}} for purpose in ("core", "cash_control")}
    for row in result["records"]["trades"]:
        purpose = row.get("order_purpose") if arm_key == "matched_original" else "core"
        if purpose not in costs:
            raise ValueError("Every matched fill needs a declared core or cash-control purpose")
        costs[purpose]["fill_count"] += 1
        for key in fields:
            costs[purpose][key + "_cny"] += Decimal(row[key])
    for key in fields:
        if sum((v[key + "_cny"] for v in costs.values()), Decimal(0)) != Decimal(result["metrics"][key + "_cny"]):
            raise ValueError("Purpose costs do not reconcile to all actual original ledger fills")
    return {purpose: {k: str(v) if isinstance(v, Decimal) else v for k, v in fields.items()} for purpose, fields in costs.items()}


def decide_exposure_matching(results, matching_by_period):
    paths = {(r["arm_key"], r["period"]): r for r in results}
    if len(paths) != len(results) or set(paths) != {(a, p) for a in MATCHING_ARMS for p in PERIODS} or set(matching_by_period) != set(PERIODS):
        raise ValueError("All nine fixed paths and three matching checks are required")
    if any(type(r["performance_valid"]) is not bool for r in results):
        raise ValueError("Every path requires explicit accounting validity")
    if any(type(m.get("passed")) is not bool or m["passed"] != (m.get("status") == "MATCHED") for m in matching_by_period.values()):
        raise ValueError("Matching status and explicit gate outcome must agree")
    periods = {}
    for period in PERIODS:
        base, cap, matched = [paths[(key, period)] for key in MATCHING_ARMS]
        eligible = all(x["performance_valid"] is True for x in (base, cap, matched)) and matching_by_period[period]["passed"] is True
        # This exact accounting identity describes changed full strategies,
        # including their actual costs; it is not a pure causal decomposition.
        attribution = None
        if eligible:
            original_return, cap_return, matched_return = [Decimal(x["metrics"]["total_return_lower_exact"]) for x in (base, cap, matched)]
            raw, cash, remaining = cap_return-original_return, matched_return-original_return, cap_return-matched_return
            if cash + remaining != raw:
                raise ValueError("The signed recorded net-return bridge must reconcile exactly")
            attribution = {"raw_cap75_minus_original_net": str(raw),
                "cash_control_related_net_change_including_cost_and_path": str(cash),
                "remaining_cap75_minus_matched_net_gap": str(remaining),
                "exact_additive_net_bridge": True, "pure_cash_or_turnover_causal_claim": False}
        periods[period] = {"all_three_financially_valid": all(x["performance_valid"] is True for x in (base, cap, matched)),
            "matching_status": matching_by_period[period]["status"], "eligible_for_descriptive_comparison": eligible,
            "net_return_bridge": attribution}
    all_valid = all(x["performance_valid"] is True for x in results)
    return {"status": "INCONCLUSIVE_INVALID_ORIGINAL_REFERENCE" if not all_valid else "MATCHED_DESCRIPTIVE_COMPARISON" if all(x["eligible_for_descriptive_comparison"] for x in periods.values()) else "MATCHING_NOT_PROVEN",
        "periods": periods, "current_paper_action": "keep_existing_top20_unchanged",
        "strategy_promoted": False, "model_retrained": False, "new_future_OOS_claim": False,
        "no_post_result_matching_parameter_adjustment": True}


def _write_path(result, directory, arm_key, diagnostics):
    directory.mkdir(parents=True)
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            _save(directory / (name + ".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
    _save(directory / "final_state.json", result["final_state"])
    _save(directory / "diagnostics.json", diagnostics)
    summary = {k: v for k, v in result.items() if k not in {"records", "final_state"}}
    summary.update(arm_key=arm_key, purpose_costs=purpose_costs(result, arm_key), diagnostics={k: v for k, v in diagnostics.items() if k != "daily_exposures"})
    _save(directory / "summary.json", summary)
    return summary


def run_exposure_matching_comparison(declaration, output_directory, *, progress=None):
    from stopat30m.ashare_position_count_metrics import position_count_diagnostics
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve previous experiments; choose a new output directory")
    declaration = Path(declaration).resolve(); declared = _read(declaration)
    if declared.get("kind") != KIND:
        raise ValueError("A frozen A12 declaration is required")
    _check_design(declared["design"])
    bound = {**declared["bound_inputs"], "runtime_declaration": _proof(declaration)}
    _assert_proofs(bound); _check_inheritance(declared)
    _check_output_isolation(output, declared["active_paper"])
    if any(bound.get(k) != p for k, p in implementation_proofs().items()):
        raise ValueError("Exposure matching implementation changed after declaration")
    source = declared["source"]; reference = Path(declared["reference_directory"])
    model_before = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
    output.mkdir(parents=True); shutil.copyfile(declaration, output / "declaration.json")
    results, reproductions, matching = [], {}, {}
    _save(output / "status.json", {"phase": "loading_frozen_sources"})
    try:
        context = load_stability_context(source["snapshot"], source["scope_directory"], bound["protocol"]["path"],
            bound["market_audit"]["path"], source["predictions_directory"], bound["merger_actions"]["path"])
        reference_risk = {p: _read(reference / "cap7p5pct" / p / "diagnostics.json") for p in PERIODS}
        with context["pool"].verified_batch():
            for arm in MATCHING_ARMS:
                if arm == "matched_original" and len(reproductions) != 6:
                    raise ValueError("Six exact original paths must precede every matching control")
                for period in PERIODS:
                    status = {"phase": "ledger", "arm_key": arm, "period": period}
                    _save(output / "status.json", status)
                    if progress: progress(status)
                    isolated = {k: v for k, v in context.items() if k != "score_coverage"}
                    exposures = {r["date"]: r for r in reference_risk[period]["daily_exposures"]}
                    result = simulate_exposure_matching(isolated, period=period, arm_key=arm, reference_exposures=exposures)
                    if arm != "matched_original":
                        reproductions[arm + "/" + period] = assert_matching_reference_reproduced(result, reference / arm / period)
                    diag = position_count_diagnostics(result)
                    results.append(_write_path(result, output / arm / period, arm, diag))
                    if arm == "matched_original":
                        cap = next(x for x in results if x["arm_key"] == "cap7p5pct" and x["period"] == period)
                        matching[period] = exposure_match_diagnostics(reference_risk[period], diag,
                            reference_valid=cap["performance_valid"], matched_valid=result["performance_valid"])
                        _save(output / arm / period / "matching.json", matching[period])
                    if progress: progress({**status, "completed": True, "performance_valid": result["performance_valid"]})
        _assert_proofs(bound); _assert_proofs(context["bound"])
        context["pool"].assert_unchanged(); context["actions"].assert_unchanged()
        model_after = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
        report = {"kind": "a12_lagged_exposure_matching_results", "completed_at": datetime.now(timezone.utc).isoformat(),
            "declaration": _proof(output / "declaration.json"), "results": results,
            "baseline_full_reproduction": reproductions, "matching_by_period": matching,
            "decision": decide_exposure_matching(results, matching),
            "all_performance_valid": all(x["performance_valid"] is True for x in results),
            "bound_inputs": bound, "model_verification_before": model_before, "model_verification_after": model_after,
            "native_fits": 0, "predictions_regenerated": False, "active_paper_changed_by_experiment": False,
            "live_broker_orders": 0, "limitations": declared["design"]["interpretation"]}
        for proof in implementation_proofs().values():
            p = output / "source" / Path(proof["path"]).name; p.parent.mkdir(exist_ok=True); shutil.copyfile(proof["path"], p)
        _save(output / "report.json", report)
        _save(output / "status.json", {"phase": "completed", "all_performance_valid": report["all_performance_valid"]})
        _save(output / "manifest.json", {"kind": report["kind"], "files": {
            str(p.relative_to(output)): _proof(p)["sha256"] for p in sorted(output.rglob("*")) if p.is_file()}})
        return report
    except Exception as exc:
        _save(output / "failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "completed_paths": len(results), "reproduced_paths": sorted(reproductions), "no_valid_comparison_claim": True})
        _save(output / "status.json", {"phase": "failed", "message": str(exc)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    for key in ("parent-declaration", "reference-directory", "design-path", "output"):
        freeze.add_argument("--" + key, type=Path, required=True)
    run = sub.add_parser("run"); run.add_argument("--declaration", type=Path, required=True); run.add_argument("--output-dir", type=Path, required=True)
    args = vars(parser.parse_args()); command = args.pop("command")
    if command == "freeze":
        result = freeze_exposure_matching_declaration(**args); print(json.dumps({"kind": result["kind"], "declared_at": result["declared_at"]}))
    else:
        result = run_exposure_matching_comparison(args["declaration"], args["output_dir"], progress=lambda r: print(json.dumps(r), flush=True))
        print(json.dumps(result["decision"]))
        if result["all_performance_valid"] is not True:
            parser.exit(1, "Invalid historical paths preserved; no strategy switch claim.\n")


if __name__ == "__main__":
    main()
