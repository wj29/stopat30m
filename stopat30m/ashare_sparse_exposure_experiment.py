"""Frozen A13 sparse cash-control comparison; never changes the active paper."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd

from stopat30m.ashare_exposure_matching_experiment import (
    JSON_RECORDS, _read, _check_output_isolation, exposure_match_diagnostics, purpose_costs,
)
from stopat30m.ashare_sparse_exposure_validation import (
    CONTROL_ARMS, CANDIDATES, simulate_sparse_exposure,
)
from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.ashare_stability_validation import _assert_proofs, _proof, _save, load_stability_context
from stopat30m.ashare_turnover_experiment import PERIODS
from stopat30m.data.qlib_snapshot import guard_qlib_output_path

KIND = "a13_sparse_lagged_exposure_declaration"
RESULT_KIND = "a13_sparse_lagged_exposure_results"
DESIGN_CANONICAL_SHA256 = "d2a22b48fc3f551ef56209ff48ac1d9fcebb693b06ad054912388e86a66cbc77"


def _check_design(design):
    digest = hashlib.sha256(json.dumps(design, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if digest != DESIGN_CANONICAL_SHA256:
        raise ValueError("The entire predeclared A13 design must remain unchanged")
    expected = {c["key"]: {"cash_control_sessions": c["cash_control_actual_sessions"],
        "minimum_order_notional_cny": c["minimum_order_execution_notional_cny"]} for c in design["candidates"]}
    if list(CONTROL_ARMS) != design["control_arms"] or CANDIDATES != expected:
        raise ValueError("Runtime policies must exactly match the frozen design")


def implementation_proofs():
    from stopat30m import ashare_sparse_exposure_validation, ashare_sparse_exposure_execution
    return {key: _proof(path) for key, path in {
        "a13_experiment": __file__, "a13_validation": ashare_sparse_exposure_validation.__file__,
        "a13_execution": ashare_sparse_exposure_execution.__file__,
    }.items()}


def _check_inheritance(declared):
    parent = _read(declared["bound_inputs"]["a13_parent"]["path"])
    if parent.get("kind") != "a12_lagged_exposure_matching_declaration":
        raise ValueError("The frozen A12 parent is required")
    if any(declared[k] != parent[k] for k in ("source", "active_paper")):
        raise ValueError("Inherited source and active paper must match parent")
    if any(declared["bound_inputs"].get(k) != p for k,p in parent["bound_inputs"].items()):
        raise ValueError("Every inherited source proof must remain unchanged")
    reference = Path(declared["reference_directory"]).resolve()
    if (Path(declared["bound_inputs"]["a13_reference_report"]["path"]).parent != reference
            or _proof(reference/"declaration.json")["sha256"] != declared["bound_inputs"]["a13_parent"]["sha256"]):
        raise ValueError("Reference ledgers must use the exact frozen parent execution")


def freeze_sparse_exposure_declaration(*, parent_declaration, reference_directory, design_path, output):
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve previous declarations; choose a new output path")
    parent_path, design_path = Path(parent_declaration).resolve(), Path(design_path).resolve()
    parent, design = _read(parent_path), _read(design_path)
    _check_design(design)
    if parent.get("kind") != "a12_lagged_exposure_matching_declaration":
        raise ValueError("The frozen A12 parent is required")
    _check_output_isolation(output, parent["active_paper"])
    reference = Path(reference_directory).resolve(); inventory = _read(reference/"manifest.json")
    if inventory.get("kind") != "a12_lagged_exposure_matching_results":
        raise ValueError("The complete frozen A12 reference is required")
    if _proof(reference/"declaration.json")["sha256"] != _proof(parent_path)["sha256"]:
        raise ValueError("Reference declaration differs from parent")
    bound = {**parent["bound_inputs"], **implementation_proofs(),
        "a13_parent": _proof(parent_path), "a13_design": _proof(design_path),
        "a13_reference_report": _proof(reference/"report.json"),
        "a13_reference_manifest": _proof(reference/"manifest.json")}
    for name,digest in inventory["files"].items():
        path = (reference/name).resolve()
        if not path.is_relative_to(reference):
            raise ValueError("Reference artifact escapes its directory")
        bound["a13_reference:"+name] = {"path": str(path), "sha256": digest}
    _assert_proofs(bound)
    source = parent["source"]
    verified = verify_reused_stability_models(source["predictions_directory"], bound["protocol"]["path"], bound["market_audit"]["path"])
    paper = Path(parent["active_paper"]); active = _read(paper/"manifest.json")
    if active.get("kind") != "isolated_ashare_low_turnover_forward" or active["strategy"]["top_k"] != 20:
        raise ValueError("The active paper must retain its existing top20 configuration")
    payload = {"kind": KIND, "declared_at": datetime.now(timezone.utc).isoformat(),
        "design": design, "bound_inputs": bound, "source": source,
        "reference_directory": str(reference), "model_verification": verified,
        "active_paper": str(paper), "active_state_at_declaration": _proof(paper/"state.json"),
        "active_state_is_observation_not_frozen_input": True,
        "fits_performed": 0, "predictions_regenerated": False}
    output.parent.mkdir(parents=True, exist_ok=True); _save(output,payload)
    return payload


def assert_sparse_reference_reproduced(result, directory):
    directory = Path(directory)
    expected = {k:v for k,v in _read(directory/"summary.json").items()
        if k not in {"arm_key", "purpose_costs", "diagnostics"}}
    actual = {k:v for k,v in result.items() if k not in {"records", "final_state"}}
    if actual != expected or result["final_state"] != _read(directory/"final_state.json"):
        raise ValueError("Full original core summary or final state differs")
    names = {p.stem for p in directory.glob("*.csv")} | {
        p.stem for p in directory.glob("*.json") if p.stem in JSON_RECORDS}
    if set(result["records"]) != names:
        raise ValueError("Original canonical record inventory differs")
    proofs = {}
    for name,rows in result["records"].items():
        path = directory/(name+(".json" if name in JSON_RECORDS else ".csv"))
        same = rows == _read(path) if name in JSON_RECORDS else (
            hashlib.sha256(pd.DataFrame(rows).to_csv(index=False).encode()).hexdigest() == _proof(path)["sha256"])
        if not same:
            raise ValueError("Full original record differs: "+name)
        proofs[name] = _proof(path)
    return {"passed": True, "performance_valid": result["performance_valid"],
        "record_types": sorted(proofs), "canonical_records": proofs}


def decide_sparse_exposure(results, matching_by_arm):
    arms = (*CONTROL_ARMS, *CANDIDATES)
    paths = {(r["arm_key"],r["period"]):r for r in results}
    if len(paths) != len(results) or set(paths) != {(a,p) for a in arms for p in PERIODS}:
        raise ValueError("All 21 distinct fixed paths are required")
    if set(matching_by_arm) != {"matched_original",*CANDIDATES} or any(set(m) != set(PERIODS) for m in matching_by_arm.values()):
        raise ValueError("Every daily and sparse control needs three explicit matching checks")
    if any(type(r["performance_valid"]) is not bool for r in results):
        raise ValueError("Every path requires explicit financial validity")
    for matches in matching_by_arm.values():
        if any(type(m.get("passed")) is not bool or m["passed"] != (m.get("status") == "MATCHED") for m in matches.values()):
            raise ValueError("Matching status and explicit gate outcome must agree")
    comparisons = []
    for arm in CANDIDATES:
        periods = {}
        for period in PERIODS:
            original,cap,daily,candidate = [paths[(a,period)] for a in (*CONTROL_ARMS,arm)]
            matched = matching_by_arm[arm][period]["passed"]
            valid = all(a["performance_valid"] for a in (original,cap,daily,candidate))
            eligible = valid and matched and matching_by_arm["matched_original"][period]["passed"]
            checks = {k:None for k in ("net_no_lower_than_daily", "conservative_drawdown_no_worse_than_daily",
                "friction_strictly_lower_than_daily", "cash_control_fill_count_strictly_lower_than_daily")}
            bridge = None
            if eligible:
                m,c = daily["metrics"], candidate["metrics"]
                checks.update(net_no_lower_than_daily=Decimal(c["total_return_lower_exact"]) >= Decimal(m["total_return_lower_exact"]),
                    conservative_drawdown_no_worse_than_daily=Decimal(c["max_drawdown_conservative_exact"]) >= Decimal(m["max_drawdown_conservative_exact"]),
                    friction_strictly_lower_than_daily=sum(Decimal(c[k]) for k in ("total_fee_cny","slippage_cost_cny")) < sum(Decimal(m[k]) for k in ("total_fee_cny","slippage_cost_cny")),
                    cash_control_fill_count_strictly_lower_than_daily=candidate["purpose_costs"]["cash_control"]["fill_count"] < daily["purpose_costs"]["cash_control"]["fill_count"])
                with localcontext() as precision:
                    precision.prec = 80
                    a,b,x = [Decimal(row["metrics"]["total_return_lower_exact"]) for row in (original,cap,candidate)]
                    bridge = {"raw_cap75_minus_original_net": str(b-a),
                        "candidate_cash_control_related_net_change_including_cost_and_path": str(x-a),
                        "remaining_cap75_minus_candidate_net_gap": str(b-x),
                        "exact_additive_net_bridge": (x-a)+(b-x)==b-a, "pure_cash_or_turnover_causal_claim":False}
                    if not bridge["exact_additive_net_bridge"]:
                        raise ValueError("The net accounting bridge must reconcile exactly")
            periods[period] = {"all_four_financially_valid":valid,
                "candidate_matching_status":matching_by_arm[arm][period]["status"],
                "eligible_for_descriptive_comparison":bool(eligible), "checks":checks,
                "descriptive_net_bridge":bridge, "all_period_checks_passed":eligible and all(v is True for v in checks.values())}
        comparisons.append({"arm_key":arm,"sparse_policy":CANDIDATES[arm],"periods":periods,
            "isolated_period_diagnostic_passed":all(periods[p]["all_period_checks_passed"] for p in ("2025","2026")),
            "qualified_for_automatic_promotion":False})
    return {"status":"INCONCLUSIVE_INVALID_REFERENCE" if not all(r["performance_valid"] for r in results)
        else "FIXED_EXPLORATORY_COMPARISON_COMPLETE", "comparisons":comparisons,
        "current_paper_action":"keep_existing_top20_unchanged", "strategy_promoted":False,
        "model_retrained":False,"new_future_OOS_claim":False,"no_post_result_parameter_adjustment":True}


def _write_path(result,directory,arm_key,diagnostics):
    directory.mkdir(parents=True)
    for name,rows in result["records"].items():
        if name in JSON_RECORDS:_save(directory/(name+".json"),rows)
        else:pd.DataFrame(rows).to_csv(directory/(name+".csv"),index=False)
    _save(directory/"final_state.json",result["final_state"]);_save(directory/"diagnostics.json",diagnostics)
    summary = {k:v for k,v in result.items() if k not in {"records","final_state"}}
    summary.update(arm_key=arm_key,sparse_policy=CANDIDATES.get(arm_key),
        purpose_costs=purpose_costs(result,"matched_original" if arm_key in CANDIDATES else arm_key),
        diagnostics={k:v for k,v in diagnostics.items() if k != "daily_exposures"})
    _save(directory/"summary.json",summary)
    return summary


def run_sparse_exposure_comparison(declaration,output_directory,*,progress=None):
    from stopat30m.ashare_position_count_metrics import position_count_diagnostics
    output = guard_qlib_output_path(output_directory)
    if output.exists():raise ValueError("Preserve previous experiments; choose a new output directory")
    declaration=Path(declaration).resolve();declared=_read(declaration)
    if declared.get("kind") != KIND:raise ValueError("A frozen A13 declaration is required")
    _check_design(declared["design"])
    bound={**declared["bound_inputs"],"runtime_declaration":_proof(declaration)}
    _assert_proofs(bound);_check_inheritance(declared);_check_output_isolation(output,declared["active_paper"])
    if any(bound.get(k)!=p for k,p in implementation_proofs().items()):
        raise ValueError("Sparse exposure implementation changed after declaration")
    source=declared["source"];reference=Path(declared["reference_directory"])
    before=verify_reused_stability_models(source["predictions_directory"],bound["protocol"]["path"],bound["market_audit"]["path"])
    output.mkdir(parents=True);shutil.copyfile(declaration,output/"declaration.json")
    results,reproductions,matching=[],{},{}
    _save(output/"status.json",{"phase":"loading_frozen_sources"})
    try:
        context=load_stability_context(source["snapshot"],source["scope_directory"],bound["protocol"]["path"],
            bound["market_audit"]["path"],source["predictions_directory"],bound["merger_actions"]["path"])
        reference_risk={p:_read(reference/"cap7p5pct"/p/"diagnostics.json") for p in PERIODS}
        with context["pool"].verified_batch():
            for arm in (*CONTROL_ARMS,*CANDIDATES):
                if arm in CANDIDATES and len(reproductions)!=9:
                    raise ValueError("Nine exact original paths must precede every new sparse arm")
                for period in PERIODS:
                    status={"phase":"ledger","arm_key":arm,"period":period};_save(output/"status.json",status)
                    if progress:progress(status)
                    isolated={k:v for k,v in context.items() if k!="score_coverage"}
                    exposures={r["date"]:r for r in reference_risk[period]["daily_exposures"]}
                    result=simulate_sparse_exposure(isolated,period=period,arm_key=arm,reference_exposures=exposures)
                    if arm in CONTROL_ARMS:
                        reproductions[arm+"/"+period]=assert_sparse_reference_reproduced(result,reference/arm/period)
                    diag=position_count_diagnostics(result)
                    results.append(_write_path(result,output/arm/period,arm,diag))
                    if arm=="matched_original" or arm in CANDIDATES:
                        cap=next(r for r in results if r["arm_key"]=="cap7p5pct" and r["period"]==period)
                        check=exposure_match_diagnostics(reference_risk[period],diag,
                            reference_valid=cap["performance_valid"],matched_valid=result["performance_valid"])
                        matching.setdefault(arm,{})[period]=check;_save(output/arm/period/"matching.json",check)
                    if progress:progress({**status,"completed":True,"performance_valid":result["performance_valid"]})
        _assert_proofs(bound);_assert_proofs(context["bound"]);context["pool"].assert_unchanged();context["actions"].assert_unchanged()
        after=verify_reused_stability_models(source["predictions_directory"],bound["protocol"]["path"],bound["market_audit"]["path"])
        report={"kind":RESULT_KIND,"completed_at":datetime.now(timezone.utc).isoformat(),
            "declaration":_proof(output/"declaration.json"),"results":results,
            "baseline_full_reproduction":reproductions,"matching_by_arm":matching,
            "decision":decide_sparse_exposure(results,matching),
            "all_performance_valid":all(r["performance_valid"] for r in results),
            "bound_inputs":bound,"model_verification_before":before,"model_verification_after":after,
            "native_fits":0,"predictions_regenerated":False,"active_paper_changed_by_experiment":False,
            "live_broker_orders":0,"limitations":declared["design"]["interpretation"]}
        for proof in implementation_proofs().values():
            path=output/"source"/Path(proof["path"]).name;path.parent.mkdir(exist_ok=True);shutil.copyfile(proof["path"],path)
        _save(output/"report.json",report);_save(output/"status.json",{"phase":"completed","all_performance_valid":report["all_performance_valid"]})
        _save(output/"manifest.json",{"kind":RESULT_KIND,"files":{str(p.relative_to(output)):_proof(p)["sha256"]
            for p in sorted(output.rglob("*")) if p.is_file()}})
        return report
    except Exception as exc:
        _save(output/"failure.json",{"exception_type":type(exc).__name__,"message":str(exc),
            "completed_paths":len(results),"reproduced_paths":sorted(reproductions),"no_valid_comparison_claim":True})
        _save(output/"status.json",{"phase":"failed","message":str(exc)});raise


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest="command",required=True)
    freeze=sub.add_parser("freeze")
    for key in ("parent-declaration","reference-directory","design-path","output"):
        freeze.add_argument("--"+key,type=Path,required=True)
    run=sub.add_parser("run");run.add_argument("--declaration",type=Path,required=True);run.add_argument("--output-dir",type=Path,required=True)
    args=vars(parser.parse_args());command=args.pop("command")
    if command=="freeze":
        r=freeze_sparse_exposure_declaration(**args);print(json.dumps({"kind":r["kind"],"declared_at":r["declared_at"]}))
    else:
        r=run_sparse_exposure_comparison(args["declaration"],args["output_dir"],progress=lambda x:print(json.dumps(x),flush=True))
        print(json.dumps(r["decision"]))
        if not r["all_performance_valid"]:parser.exit(1,"Invalid historical paths preserved; no strategy switch claim.\n")


if __name__=="__main__":main()
