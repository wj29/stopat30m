"""A16 fixed-score, costed comparisons with no fit or paper-account mutation.

All twelve prior model paths must reproduce before any new control is run.
Source families and their separate continuous accounts never get concatenated.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import ashare_model_debug_research as training
from . import ashare_model_debug_experiment as prior
from . import ashare_stability_validation as stability
from . import ashare_model_baseline_validation as validation
from . import ashare_model_baseline_signal as signal
from .ashare_low_turnover_recovery import verify_recovery_binding

KIND = "a16_fixed_score_baseline_execution"
DESIGN_SHA256 = "21cb47626c639f8cbc69ed16602efc7845ffe21620e45351907ec3a634e43a61"
STRATEGIES = ("original", "regularized300", "MOM60", "REV20",
              "HASH1103", "HASH2207", "HASH3301", "HASH4409", "HASH5519")
PERIODS = {"primary": ("2023", "2024", "continuous"),
           "stability": ("2025", "2026", "continuous")}
MODELS = STRATEGIES[:2]
JSON_RECORDS = prior.JSON_RECORDS
RECORDS = {"curves", "trades", "orders", "positions", "claims", "signals", "corporate_actions",
           "plans", "valuation_audit", "score_coverage", "holding_age", "holding_origin_actions", "turnover_plans"}
BASE = Path("output/research/ashare")


def _read(path):
    return json.loads(Path(path).read_bytes())


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _proof(path):
    return stability._proof(Path(path).resolve())


def _save_new(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")


def _check_design(design):
    if hashlib.sha256(_canonical(design)).hexdigest() != DESIGN_SHA256:
        raise ValueError("The complete pre-result A16 design must remain unchanged")


def _isolated(path, paper, *, exists=False):
    target = stability.guard_qlib_output_path(path)
    paper = Path(paper).resolve()
    if ("forward" in target.parts or "models" in target.parts or target == paper
            or target.is_relative_to(paper) or paper.is_relative_to(target)):
        raise ValueError("Offline outputs cannot target paper or model directories")
    if not exists and target.exists():
        raise FileExistsError("Preserve every previous A16 output")
    return target


def _verify_historical_declaration(path):
    """Keep the old declaration intact across its explicit A15 paper revision.

    The sole superseded live-manifest observation is verified against the exact
    immutable archive named by that revision. Every other old proof is checked
    at its original location, including every training/execution source.
    """
    declaration, _ = training._load_declaration(path)
    paper = Path(declaration["active_paper_directory"]).resolve()
    manifest, state = _read(paper / "manifest.json"), _read(paper / "state.json")
    verify_recovery_binding(paper, manifest, state)
    historical = declaration["bound_inputs"]["active_paper_manifest"]
    if declaration["active_paper"] != historical or Path(historical["path"]) != paper / "manifest.json":
        raise ValueError("The old active-manifest observation is ambiguous")
    archive = manifest.get("engineering_revision", {}).get("parent_manifest")
    if archive is None or archive["sha256"] != historical["sha256"]:
        raise ValueError("The explicit same-account revision must archive the exact old declaration")
    training._assert_proof(archive)
    for key, value in declaration["bound_inputs"].items():
        if key != "active_paper_manifest":
            training._assert_proof(value)
    old = _read(archive["path"])
    if (old["model_sha256"] != manifest["model_sha256"] or old["strategy"] != manifest["strategy"]
            or old["candidate_proofs"] != manifest["candidate_proofs"]):
        raise ValueError("The current paper must retain its native model and strategy")
    for filename, digest in manifest["implementation_sha256"].items():
        training._assert_proof({"path": filename, "sha256": digest})
    return declaration, {"historical_active_manifest": archive,
        "current_active_manifest": _proof(paper / "manifest.json"),
        "paper_revision": manifest["engineering_revision"]["event"],
        "unchanged_original_source_proofs_checked": len(declaration["bound_inputs"]) - 1}


def _packages():
    return {phase: (BASE / "models" / f"a14_model_debug_{phase}_20261021_v1").resolve()
            for phase in ("selection", "confirmation", "retrospective")}


def _package_for(family, period, strategy, packages):
    if strategy != "regularized300":
        return None
    if family == "stability":
        return packages["retrospective"]
    if period == "continuous":
        return [packages["selection"], packages["confirmation"]]
    return packages["selection" if period == "2023" else "confirmation"]


def _reference(family, period, strategy):
    phase = "retrospective" if family == "stability" else "selection" if period == "2023" else "confirmation"
    candidate = "baseline" if strategy == "original" else "regularized300"
    return (BASE / "experiments" / f"a14_model_debug_{phase}_fee_validation_20261021_v1" / candidate / period).resolve()


def implementation_proofs():
    return {name: _proof(module.__file__) for name, module in
        (("experiment", __import__(__name__, fromlist=["__file__"])),
         ("baseline_validation", validation), ("baseline_signal", signal),
         ("original_debug_validation", __import__("stopat30m.ashare_model_debug_validation", fromlist=["__file__"])),
         ("original_debug_experiment", prior))}


def freeze_baseline_declaration(design_path, output):
    design = _read(design_path); _check_design(design)
    source_path = Path(design["source_training_declaration"]).resolve()
    declaration, paper_revision = _verify_historical_declaration(source_path)
    output = _isolated(output, declaration["active_paper_directory"])
    packages = _packages()
    bound = {"design": _proof(design_path), "a14_training_declaration": _proof(source_path),
             **implementation_proofs(), **{k:v for k,v in paper_revision.items() if isinstance(v, dict)}}
    for phase, package in packages.items():
        training.verify_debug_model_package(package, source_path)
        if _read(package / "manifest.json")["phase"] != phase:
            raise ValueError("A package must belong to its declared chronological phase")
        bound["models:" + phase] = _proof(package / "manifest.json")
    for family, periods in PERIODS.items():
        for period in periods:
            for strategy in MODELS:
                directory = _reference(family, period, strategy)
                required = {"summary.json", "final_state.json"} | {
                    name + (".json" if name in JSON_RECORDS else ".csv") for name in RECORDS}
                if not all((directory/name).is_file() for name in required):
                    raise ValueError("Every original ledger needs its full canonical inventory before freezing")
                for path in sorted(directory.iterdir()):
                    if path.is_file():
                        bound[f"reference:{family}:{period}:{strategy}:{path.name}"] = _proof(path)
    diagnostic = BASE/"experiments/a14_model_diagnostics_20261021_v1"
    for name, digest in _read(diagnostic/"signal_completion_manifest.json")["files"].items():
        training._assert_proof({"path":str((diagnostic/name).resolve()),"sha256":digest})
    for name in ("signal_completion_manifest.json", "signal_daily.csv"):
        bound["original_signal:"+name] = _proof(diagnostic/name)
    training._assert_proofs(bound)
    payload = {"kind": KIND, "schema_version": 1,
        "declared_at": datetime.now(timezone.utc).isoformat(), "bound_inputs": bound,
        "design": design, "training_declaration": str(source_path),
        "packages": {k:str(v) for k,v in packages.items()},
        "active_paper_directory": declaration["active_paper_directory"],
        "historical_paper_binding": paper_revision, "new_returns_before_declaration": False,
        "new_native_fits": 0, "new_native_predictions": 0, "broker_orders": 0}
    _save_new(output, payload)
    return payload


def _load_declaration(path):
    declaration = _read(path)
    if declaration.get("kind") != KIND or declaration.get("schema_version") != 1:
        raise ValueError("An explicitly frozen A16 execution declaration is required")
    _check_design(declaration["design"])
    training._assert_proofs(declaration["bound_inputs"])
    if declaration["design"] != _read(declaration["bound_inputs"]["design"]["path"]):
        raise ValueError("The design bytes and declaration differ")
    if any(declaration["bound_inputs"].get(k) != v for k,v in implementation_proofs().items()):
        raise ValueError("A16 implementation differs from its pre-result declaration")
    if (declaration["bound_inputs"].get("a14_training_declaration") != _proof(declaration["training_declaration"])
            or declaration.get("new_returns_before_declaration") is not False
            or set(declaration.get("packages", {})) != {"selection", "confirmation", "retrospective"}):
        raise ValueError("Execution pointers and the pre-result phase contract must remain bound")
    for phase, package in declaration["packages"].items():
        if declaration["bound_inputs"].get("models:"+phase) != _proof(Path(package)/"manifest.json"):
            raise ValueError("A model-package pointer differs from its frozen proof")
        training.verify_debug_model_package(package,declaration["training_declaration"])
        if _read(Path(package)/"manifest.json")["phase"] != phase:
            raise ValueError("A package must belong to its declared chronological phase")
    source, binding = _verify_historical_declaration(declaration["training_declaration"])
    if binding != declaration["historical_paper_binding"]:
        raise ValueError("The source-model and same-account paper revision binding changed")
    if declaration.get("active_paper_directory") != source["active_paper_directory"]:
        raise ValueError("The active-paper isolation boundary changed")
    if any(declaration.get(k) != 0 for k in ("new_native_fits", "new_native_predictions", "broker_orders")):
        raise ValueError("A16 cannot train, predict, promote, or access a broker")
    return declaration


def assert_a14_reproduced(result, directory):
    directory = Path(directory)
    expected = _read(directory / "summary.json")
    actual = {k:v for k,v in result.items() if k not in {"records", "final_state", "candidate"}}
    expected = {k:v for k,v in expected.items() if k != "candidate"}
    if _canonical(actual) != _canonical(expected) or _canonical(result["final_state"]) != _canonical(_read(directory / "final_state.json")):
        raise ValueError("The entire A14 summary and final state must reproduce exactly")
    names = {p.stem for p in directory.glob("*.csv")} | {
        p.stem for p in directory.glob("*.json") if p.stem in JSON_RECORDS}
    if set(result["records"]) != names:
        raise ValueError("A14 record inventory differs")
    for name, rows in result["records"].items():
        path = directory / (name + (".json" if name in JSON_RECORDS else ".csv"))
        if name in JSON_RECORDS:
            equal = _canonical(rows) == _canonical(_read(path))
        else:
            equal = pd.DataFrame(rows).to_csv(index=False).encode() == path.read_bytes()
        if not equal:
            raise ValueError("A14 canonical record bytes differ: " + name)
    return {"passed": True, "summary": _proof(directory / "summary.json"),
            "all_record_types": sorted(names), "comparison": "summary_finalstate_all_JSON_CSV_exact_including_invalidity"}


def _finite_metric(row, key):
    value = Decimal(str(row["metrics"][key]))
    if not value.is_finite():
        raise ValueError("Finite financial metrics are required")
    return value


def summarize_financial_paths(results):
    expected = {(f,p,s) for f,ps in PERIODS.items() for p in ps for s in STRATEGIES}
    indexed = {}
    for row in results:
        key = (row["family"], row["period"], row["strategy"])
        if key in indexed or type(row.get("performance_valid")) is not bool:
            raise ValueError("Every unique predeclared path requires explicit financial validity")
        for metric in ("total_return_lower_exact", "max_drawdown_conservative_exact"):
            _finite_metric(row, metric)
        indexed[key] = row
    if set(indexed) != expected:
        raise ValueError("All 54 fixed paths, including invalid controls, must be reported")
    pairs, hashes = [], []
    for family, periods in PERIODS.items():
        for period in periods:
            random = [indexed[family,period,s] for s in STRATEGIES if s.startswith("HASH")]
            valid = [row for row in random if row["performance_valid"]]
            distribution = {"family":family,"period":period,"fixed_seed_count":5,"valid_count":len(valid),
                "invalid_strategies":[row["strategy"] for row in random if not row["performance_valid"]],
                "all_five_valid":len(valid)==5,"conditional_on_valid_paths":len(valid)!=5,
                "pvalue":None,"best_seed_selected":False}
            for field in ("total_return_lower_exact","max_drawdown_conservative_exact"):
                values = np.asarray([float(_finite_metric(row,field)) for row in valid])
                distribution[field] = {name:float(fun(values)) if len(values) else None
                    for name,fun in (("mean",np.mean),("median",np.median),("min",np.min),("max",np.max))}
            hashes.append(distribution)
            for model in MODELS:
                left = indexed[family,period,model]
                for control in ("MOM60","REV20"):
                    right = indexed[family,period,control]
                    ok = left["performance_valid"] and right["performance_valid"]
                    pairs.append({"family":family,"period":period,"model":model,"control":control,
                        "comparison_allowed":ok,"model_valid":left["performance_valid"],"control_valid":right["performance_valid"],
                        "net_return_difference":float(_finite_metric(left,"total_return_lower_exact")-_finite_metric(right,"total_return_lower_exact")) if ok else None,
                        "drawdown_difference":float(_finite_metric(left,"max_drawdown_conservative_exact")-_finite_metric(right,"max_drawdown_conservative_exact")) if ok else None,
                        "risk_and_exposure_matched":False})
    return {"paths":len(results),"financially_valid_paths":sum(r["performance_valid"] for r in results),
        "paired_models_vs_simple":pairs,"fixed_hash_distributions":hashes,
        "new_future_OOS_or_pure_alpha_claim":False,"retraining_decision_requires_joint_evidence":True}


def _write_result(result, directory, family, strategy):
    summary = prior._write_result(result, directory, strategy)
    return {**summary,"family":family,"strategy":strategy,"directory":str(directory)}


def run_baseline_comparison(declaration_path, output, *, progress=None):
    declaration = _load_declaration(declaration_path)
    output = _isolated(output, declaration["active_paper_directory"])
    output.mkdir(parents=True)
    _save_new(output / "declaration.json", declaration)
    packages = {k:Path(v) for k,v in declaration["packages"].items()}
    contexts, results, reproductions = {}, [], {}
    def note(value):
        stability._save(output / "status.json", value)
        if progress: progress(value)
    try:
        for family, periods in PERIODS.items():
            note({"stage":"load_verified_context","family":family})
            context = validation.load_baseline_context(declaration["training_declaration"], family)
            contexts[family] = context
            with context["pool"].verified_batch():
                for strategy in MODELS:
                    for period in periods:
                        note({"stage":"reproduce_model_path","family":family,"strategy":strategy,"period":period})
                        result = validation.simulate_baseline_strategy(context,strategy=strategy,period=period,
                            prediction_package=_package_for(family,period,strategy,packages))
                        reference = _reference(family,period,strategy)
                        if "score_quality" in _read(reference / "summary.json"):
                            result["score_quality"] = stability.score_quality(context,model="LGB",period=period)
                        key = f"{family}:{period}:{strategy}"
                        reproductions[key] = assert_a14_reproduced(result,reference)
                        results.append(_write_result(result,output/family/strategy/period,family,strategy))
        if len(reproductions) != 12 or not all(x["passed"] for x in reproductions.values()):
            raise ValueError("All twelve exact model replays must pass before new controls")
        _save_new(output/"all12_reproduced_before_controls.json", {
            "completed_at":datetime.now(timezone.utc).isoformat(),"new_control_ledgers":0,"reproductions":reproductions})
        signal_checks={}
        for family,periods in PERIODS.items():
            context=contexts[family]
            with context["pool"].verified_batch():
                original=signal.diagnose_scores(context,score_sets={"original":context["scores"]["LGB"]},periods=periods)
            checked={period:signal.assert_quality_reference(original["daily"],
                _read(_reference(family,period,"original")/"summary.json")["score_quality"],period=period)
                for period in periods}
            if family=="stability":
                checked["original_A14_group_days"]=signal.assert_original_reference(original["daily"],
                    pd.read_csv(declaration["bound_inputs"]["original_signal:signal_daily.csv"]["path"]))
            signal_checks[family]=checked
        _save_new(output/"original_signal_reproduction.json",signal_checks)
        for family, periods in PERIODS.items():
            context = contexts[family]
            with context["pool"].verified_batch():
                for strategy in STRATEGIES[2:]:
                    for period in periods:
                        note({"stage":"costed_control","family":family,"strategy":strategy,"period":period,
                              "completed_ledgers":len(results)})
                        result = validation.simulate_baseline_strategy(context,strategy=strategy,period=period)
                        results.append(_write_result(result,output/family/strategy/period,family,strategy))
                score_sets = {strategy:validation.baseline_signal_scores(context,strategy,period="continuous",
                    prediction_package=_package_for(family,"continuous",strategy,packages)) for strategy in STRATEGIES}
                note({"stage":"signal_diagnosis","family":family})
                diagnosed = signal.diagnose_scores(context,score_sets=score_sets,periods=periods,original_key="original")
                directory=output/family/"signal_diagnostics";directory.mkdir()
                for name,value in diagnosed.items():
                    if isinstance(value,pd.DataFrame): value.to_csv(directory/(name+".csv"),index=False,float_format="%.17g")
                    else: _save_new(directory/(name+".json"),value)
            training._assert_proofs(context["bound"])
            context["pool"].assert_unchanged();context["actions"].assert_unchanged()
        report = {"kind":KIND,"completed_at":datetime.now(timezone.utc).isoformat(),
            "execution_declaration":_proof(declaration_path),"results":results,
            "reproductions":reproductions,"signal_reproductions":signal_checks,
            "financial_summary":summarize_financial_paths(results),
            "new_native_fits":0,"new_native_predictions":0,"broker_orders":0,
            "active_paper_mutated_by_experiment":False,"automatic_model_promotion":False,
            "all_historical_periods_previously_viewed":True,"future_OOS_claim":False}
        _load_declaration(declaration_path)
        _save_new(output/"report.json",report)
        note({"stage":"completed","ledgers":len(results),"new_fits":0,"new_predictions":0})
        _save_new(output/"manifest.json",{"kind":KIND,"bound_inputs":declaration["bound_inputs"],
            "execution_declaration":_proof(declaration_path),"files":{str(p.relative_to(output)):_proof(p)["sha256"]
                for p in sorted(output.rglob("*")) if p.is_file()}})
        return report
    except Exception as exc:
        _save_new(output/"failure.json",{"type":type(exc).__name__,"message":str(exc),
            "completed_ledgers":len(results),"completed_reproductions":sorted(reproductions),
            "no_new_model_or_promotion_claim":True})
        note({"stage":"failed","error":str(exc),"completed_ledgers":len(results)})
        raise


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest="command",required=True)
    freeze=commands.add_parser("freeze");freeze.add_argument("--design",type=Path,required=True);freeze.add_argument("--output",type=Path,required=True)
    run=commands.add_parser("run");run.add_argument("--declaration",type=Path,required=True);run.add_argument("--output",type=Path,required=True)
    args=parser.parse_args(argv)
    if args.command=="freeze":
        value=freeze_baseline_declaration(args.design,args.output);print(json.dumps({"status":"frozen","path":str(args.output),"kind":value["kind"]}))
    else:
        report=run_baseline_comparison(args.declaration,args.output,
            progress=lambda value:print(json.dumps(value,ensure_ascii=False),flush=True))
        print(json.dumps(report["financial_summary"],ensure_ascii=False))


if __name__=="__main__":main()
