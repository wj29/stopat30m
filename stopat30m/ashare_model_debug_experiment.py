"""Staged, source-bound A14 model debugging with an unchanged twenty-name ledger.

This module never trains, predicts, promotes a live model or changes the paper
account. Selection consumes 2023 only, then its immutable decision gates later
fit packages and all fee-inclusive comparisons.
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
from stopat30m.ashare_turnover_experiment import JSON_RECORDS
from stopat30m.data.qlib_snapshot import guard_qlib_output_path

KIND = "a14_model_debug_training_protocol"
EXECUTION_KIND = "a14_model_debug_execution_declaration"
DECISION_KIND = "a14_model_debug_selection_decision"
DESIGN_CANONICAL_SHA256 = "bdd3f763e19e38fca6328e4a515c236e09510a1c317e0af1d18cfe9528dbeba5"
CANDIDATES = ("capacity300", "regularized300", "recent3_capacity300")
PHASES = ("selection", "confirmation", "retrospective")
EXPECTED_MODEL_KIND = {"primary": "a5_union_fixed_predictions", "stability": "a5_union_stability_fixed_predictions"}
DEFAULT_BASELINE = Path("output/research/ashare/experiments/a6_lgb_turnover_fee_validation_20261020_v1")


def _read(path):
    value = json.loads(Path(path).read_bytes())
    if not isinstance(value, dict):
        raise ValueError("A source-bound JSON object is required")
    return value


def _canonical(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _assert_design(design):
    if _canonical(design) != DESIGN_CANONICAL_SHA256:
        raise ValueError("The entire frozen model-debug design must remain unchanged")


def _collect_proofs(value, prefix=""):
    if isinstance(value, dict):
        if isinstance(value.get("path"), str) and isinstance(value.get("sha256"), str):
            return {prefix: {"path": value["path"], "sha256": value["sha256"]}}
        result = {}
        for key, item in value.items():
            result.update(_collect_proofs(item, prefix + ":" + str(key)))
        return result
    if isinstance(value, list):
        result = {}
        for i, item in enumerate(value):
            result.update(_collect_proofs(item, prefix + ":" + str(i)))
        return result
    return {}


def _package_proofs(root, manifest, prefix):
    root = Path(root).resolve()
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("A complete original package file inventory is required")
    all_paths = list(root.rglob("*"))
    if any(path.is_symlink() for path in all_paths):
        raise ValueError("Frozen model packages cannot contain symbolic links")
    actual_names = {str(path.relative_to(root)) for path in all_paths
        if path.is_file() and path != root / "manifest.json"}
    if actual_names != set(files):
        raise ValueError("Frozen package inventory omits or invents files")
    result = {prefix + ":manifest": stability._proof(root / "manifest.json")}
    for name, digest in files.items():
        path = (root / name).resolve()
        if Path(name).is_absolute() or ".." in Path(name).parts or not path.is_relative_to(root):
            raise ValueError("Model artifact escapes its original package")
        result[prefix + ":" + name] = {"path": str(path), "sha256": digest}
    stability._assert_proofs(result)
    return result


def implementation_proofs():
    from stopat30m import ashare_model_debug_research as research
    from stopat30m import ashare_model_debug_validation as validation
    from stopat30m import ashare_turnover_experiment as turnover
    return {**stability.implementation_proofs(), **turnover.implementation_proofs(),
        "model_debug_training": stability._proof(research.__file__),
        "model_debug_validation": stability._proof(validation.__file__),
        "model_debug_experiment": stability._proof(__file__)}


def implementation_sha256():
    from stopat30m import ashare_model_debug_research as research
    return {**research.implementation_sha256(),
        **{name: proof["sha256"] for name, proof in implementation_proofs().items()}}


def _family_from_manifest(path, family):
    path = Path(path).resolve()
    manifest = _read(path)
    if manifest.get("kind") != EXPECTED_MODEL_KIND[family]:
        raise ValueError("Do not interchange the original primary and stability source families")
    inputs = manifest.get("inputs", {})
    if not {"declaration", "market_audit", "stock_basic"}.issubset(inputs):
        raise ValueError("Original model package lacks its source identity proofs")
    entry = {"model_manifest": stability._proof(path), "protocol": inputs["declaration"],
        "market_audit": inputs["market_audit"], "stock_basic": inputs["stock_basic"]}
    stability._assert_proofs(entry)
    protocol, audit = _read(entry["protocol"]["path"]), _read(entry["market_audit"]["path"])
    snapshot = Path(manifest["source_snapshot"]).resolve()
    snapshot_proof = stability._proof(snapshot / "manifest.json")
    if (snapshot_proof["sha256"] != manifest.get("source_manifest_sha256")
            or snapshot_proof["sha256"] != protocol.get("source_snapshot_manifest_sha256")):
        raise ValueError("Source family snapshot differs from its original model protocol")
    scope_proofs = _collect_proofs(manifest.get("scope_sources", {}), family + ":scope")
    scopes = {Path(p["path"]).resolve().parent for p in scope_proofs.values()
        if Path(p["path"]).name == "manifest.json"
        and p["sha256"] == protocol.get("historical_scope_manifest_sha256")}
    if len(scopes) != 1:
        raise ValueError("Exactly one original historical scope identity is required")
    scope = scopes.pop()
    bound = {**_package_proofs(path.parent, manifest, family + ":original_model"),
        **_collect_proofs(entry, family), **scope_proofs,
        **_collect_proofs(audit.get("bound_inputs", {}), family + ":market"),
        **_collect_proofs(protocol, family + ":protocol"),
        family + ":snapshot_manifest": snapshot_proof,
        family + ":scope_manifest": stability._proof(scope / "manifest.json")}
    source_files = _read(path.parent / "source_files.json")
    for relative, digest in source_files.items():
        source_path = (snapshot / "dataset" / relative).resolve()
        if Path(relative).is_absolute() or ".." in Path(relative).parts or not source_path.is_relative_to(snapshot / "dataset"):
            raise ValueError("Original feature path escapes its source snapshot")
        bound[family + ":source:" + relative] = {"path": str(source_path), "sha256": digest}
    stability._assert_proofs(bound)
    return entry, {"snapshot": str(snapshot), "scope_directory": str(scope),
        "protocol": entry["protocol"]["path"], "market_audit": entry["market_audit"]["path"],
        "predictions_directory": str(path.parent)}, bound


def _isolated_output(path, active):
    target, active = guard_qlib_output_path(path), Path(active).resolve()
    if target == active or target.is_relative_to(active) or active.is_relative_to(target):
        raise ValueError("Offline output must remain outside the active paper account")
    if target.exists():
        raise ValueError("Preserve every previous model-debug output")
    return target


def freeze_debug_declaration(*, primary_manifest, stability_manifest, design_path,
                             merger_actions, active_paper, output):
    """Seal both immutable A5 source families before the first of six fits."""
    output = _isolated_output(output, active_paper)
    design = _read(design_path)
    _assert_design(design)
    families, sources, bound = {}, {}, {}
    for name, path in (("primary", primary_manifest), ("stability", stability_manifest)):
        families[name], sources[name], proofs = _family_from_manifest(path, name)
        sources[name]["merger_actions"] = str(Path(merger_actions).resolve())
        bound.update(proofs)
    active = Path(active_paper).resolve()
    active_proof = stability._proof(active / "manifest.json")
    # Account state, runner logs and receipts are mutable observations, never
    # frozen source inputs. Manifest/model/rule/code proofs are immutable.
    active_manifest = _read(active_proof["path"])
    active_bound = _collect_proofs(active_manifest, "active_manifest")
    forbidden = {"state.json", "scheduler_log.json", "last_receipt.json"}
    active_bound = {k: v for k, v in active_bound.items() if Path(v["path"]).name not in forbidden
        and not Path(v["path"]).resolve().is_relative_to(active)}
    bound.update(active_bound)
    bound.update(implementation_proofs())
    bound.update(bound_design=stability._proof(design_path), merger_actions=stability._proof(merger_actions),
        active_paper_manifest=active_proof)
    stability._assert_proofs(bound)
    payload = {"kind": KIND, "schema_version": 1, "declared_at": datetime.now(timezone.utc).isoformat(),
        "bound_design": bound["bound_design"], "design": design,
        "source_families": families, "sources": sources, "merger_actions": bound["merger_actions"],
        "active_paper": active_proof, "active_paper_directory": str(active),
        "active_state_is_mutable_observation_not_bound_input": True,
        "implementation_sha256": implementation_sha256(), "bound_inputs": bound,
        "fits_computed_before_declaration": 0, "candidate_returns_computed_before_declaration": False,
        "live_model_change_allowed": False, "broker_orders_allowed": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    stability._save(output, payload)
    return payload


def load_debug_declaration(path):
    from stopat30m.ashare_model_debug_research import _load_declaration
    payload, design = _load_declaration(path)
    _assert_design(design)
    if (payload.get("implementation_sha256") != implementation_sha256()
            or payload.get("design") != design
            or payload.get("live_model_change_allowed") is not False
            or payload.get("broker_orders_allowed") is not False):
        raise ValueError("Model-debug code or live-account boundary changed after declaration")
    stability._assert_proofs(payload["bound_inputs"])
    if payload.get("bound_design") != payload["bound_inputs"].get("bound_design"):
        raise ValueError("Training declaration design identity conflicts with its source binding")
    if payload.get("merger_actions") != payload["bound_inputs"].get("merger_actions"):
        raise ValueError("Training declaration merger identity conflicts with its source binding")
    for family, entries in payload["source_families"].items():
        for name, proof in entries.items():
            if payload["bound_inputs"].get(family + ":" + name) != proof:
                raise ValueError("Training family identity conflicts with its original source binding")
    return payload


def _decimal_metric(row, key):
    try:
        value = Decimal(str(row["metrics"][key]))
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError("A finite exact ledger decision metric is required: " + key) from exc
    if not value.is_finite():
        raise ValueError("Nonfinite ledger decision metric: " + key)
    return value


def _indexed_results(results, expected):
    keys = [(r.get("candidate"), r.get("period")) for r in results]
    if len(set(keys)) != len(keys) or set(keys) != set(expected):
        raise ValueError("Exactly the predeclared model and period paths are required")
    for row in results:
        if type(row.get("performance_valid")) is not bool:
            raise ValueError("Financial validity must be an explicit Boolean")
        _decimal_metric(row, "total_return_lower_exact")
        _decimal_metric(row, "max_drawdown_conservative_exact")
    return dict(zip(keys, results))


def _beats(candidate, baseline):
    return (candidate["performance_valid"] is True and baseline["performance_valid"] is True
        and _decimal_metric(candidate, "total_return_lower_exact") > _decimal_metric(baseline, "total_return_lower_exact")
        and _decimal_metric(candidate, "max_drawdown_conservative_exact") >= _decimal_metric(baseline, "max_drawdown_conservative_exact"))


def decide_debug_selection(results):
    """Select using 2023 alone, with an explicit diagnostic-only fallback."""
    paths = _indexed_results(results, [(key, "2023") for key in ("baseline", *CANDIDATES)])
    baseline = paths["baseline", "2023"]
    valid = [key for key in CANDIDATES if paths[key, "2023"]["performance_valid"] is True]
    eligible = [key for key in valid if _beats(paths[key, "2023"], baseline)]
    def order(key):
        row = paths[key, "2023"]
        friction = _decimal_metric(row, "total_fee_cny") + _decimal_metric(row, "slippage_cost_cny")
        return (-_decimal_metric(row, "total_return_lower_exact"),
            -_decimal_metric(row, "max_drawdown_conservative_exact"), friction, key)
    ranked = sorted(eligible or valid, key=order)
    selected = ranked[0] if ranked else None
    return {"selected_candidate": selected, "eligible_candidates": sorted(eligible),
        "valid_candidates": sorted(valid), "ranked_candidates_under_frozen_rule": ranked,
        "primary_qualified": bool(eligible), "diagnostic_only": not bool(eligible),
        "selection_year": 2023, "later_years_used_for_selection": False,
        "confirmation_allowed": selected is not None, "live_model_selected": None,
        "selection_metrics_are_afterfee_ledger_not_rank_IC": True}


def decide_debug_confirmation(results, selection_decision):
    selected = selection_decision.get("selected_candidate")
    if selected not in CANDIDATES:
        raise ValueError("A fixed prior 2023 candidate decision is required")
    paths = _indexed_results(results, [(key, p) for key in ("baseline", selected) for p in ("2024", "continuous")])
    tests = {"selection_2023_primary_qualified": selection_decision.get("primary_qualified") is True,
        "confirmation_2024_candidate_valid_and_net_higher_DD_no_worse": _beats(paths[selected, "2024"], paths["baseline", "2024"])}
    return {"selected_candidate": selected, "tests": tests,
        "confirmation_pass": all(tests.values()), "no_reselection_from_2024": True,
        "all_confirmation_and_continuous_accounts_financially_valid": all(r["performance_valid"] is True for r in results),
        "live_model_selected": None, "retrospective_allowed_for_diagnosis": True}


def _verify_selection(path, declaration_path):
    from stopat30m.ashare_model_debug_research import phase_plan
    declaration = load_debug_declaration(declaration_path)
    declaration["_self_proof"] = stability._proof(declaration_path)
    phase_plan(declaration, phase="confirmation", selection_path=path)
    decision = _read(path)
    report = _read(decision["selection_report"]["path"])
    calculated = decide_debug_selection(report["results"])
    if any(decision.get(k) != value for k, value in calculated.items()) or report.get("selection") != calculated:
        raise ValueError("The 2023-only selection decision differs from its original complete ledger report")
    if report.get("training_declaration") != stability._proof(declaration_path):
        raise ValueError("Selection report belongs to another training declaration")
    return decision


def _new_model_proofs(directory, declaration_path, phase):
    from stopat30m.ashare_model_debug_research import verify_debug_model_package
    root = Path(directory).resolve()
    manifest = verify_debug_model_package(root, declaration_path)
    if manifest.get("phase") != phase:
        raise ValueError("Model package belongs to a different research stage")
    return manifest, _package_proofs(root, manifest, "new_model:" + phase)


def _baseline_proofs(directory):
    directory = Path(directory).resolve()
    report = _read(directory / "report.json")
    if report.get("kind") != "a6_fixed_lgb_turnover_results":
        raise ValueError("Original completed A6 low-turnover comparison is required")
    manifest = _read(directory / "manifest.json")
    return _package_proofs(directory, manifest, "old_A6")


def freeze_debug_execution_declaration(declaration_path, models_directory, output, *, phase,
                                      selection_path=None, selection_models_directory=None,
                                      baseline_directory=None):
    """Bind complete stage models and ledger code before stage returns."""
    if phase not in PHASES:
        raise ValueError("Unknown model-debug stage")
    protocol = load_debug_declaration(declaration_path)
    output = _isolated_output(output, protocol["active_paper_directory"])
    models, bound_models = _new_model_proofs(models_directory, declaration_path, phase)
    bound = {**protocol["bound_inputs"], **implementation_proofs(), **bound_models,
        "training_declaration": stability._proof(declaration_path),
        "new_model_manifest": stability._proof(Path(models_directory) / "manifest.json")}
    selection = None
    if phase == "selection":
        if selection_path is not None or selection_models_directory is not None:
            raise ValueError("2023 selection cannot consume later decisions or models")
    else:
        if selection_path is None:
            raise ValueError("Later phases require the original immutable 2023 selection")
        selection = _verify_selection(selection_path, declaration_path)
        bound["selection_decision"] = stability._proof(selection_path)
        bound.update(_collect_proofs(selection, "selection"))
        if models.get("models") != [selection["selected_candidate"]]:
            raise ValueError("Later package cannot change the chosen candidate")
        if models.get("inputs", {}).get("selection_decision") != bound["selection_decision"]:
            raise ValueError("Later model package must bind the exact prior decision")
    if phase == "confirmation":
        if selection_models_directory is None:
            raise ValueError("Continuous primary account requires both 2023 and 2024 model packages")
        _, earlier = _new_model_proofs(selection_models_directory, declaration_path, "selection")
        if stability._proof(Path(selection_models_directory) / "manifest.json") != selection["selection_model_manifest"]:
            raise ValueError("Confirmation must reuse the original selected 2023 model package")
        bound.update(earlier)
    elif selection_models_directory is not None:
        raise ValueError("Only primary confirmation consumes the separate 2023 package")
    if phase == "retrospective":
        baseline_directory = Path(baseline_directory or DEFAULT_BASELINE).resolve()
        bound.update(_baseline_proofs(baseline_directory))
        old_report = _read(baseline_directory / "report.json")
        family = protocol["source_families"]["stability"]
        for key, oldkey in (("protocol", "protocol"), ("market_audit", "market_audit"), ("model_manifest", "predictions_manifest")):
            if old_report.get("bound_inputs", {}).get(oldkey) != family[key]:
                raise ValueError("Old stability baseline source identity differs: " + key)
    elif baseline_directory is not None:
        raise ValueError("Old A6 stability reference is only used for retrospective replay")
    stability._assert_proofs(bound)
    payload = {"kind": EXECUTION_KIND, "schema_version": 1,
        "declared_at": datetime.now(timezone.utc).isoformat(), "phase": phase,
        "training_declaration": bound["training_declaration"], "bound_inputs": bound,
        "models_directory": str(Path(models_directory).resolve()),
        "selection_models_directory": str(Path(selection_models_directory).resolve()) if selection_models_directory is not None else None,
        "baseline_directory": str(baseline_directory) if baseline_directory is not None else None,
        "selected_candidate": selection["selected_candidate"] if selection else None,
        "expected_new_ledger_count": {"selection": 4, "confirmation": 4, "retrospective": 6}[phase],
        "execution": protocol["design"]["execution"], "active_paper_changed": False,
        "broker_order_sent": False, "models_verified_before_any_stage_return": True}
    output.parent.mkdir(parents=True, exist_ok=True)
    stability._save(output, payload)
    return payload


def assert_debug_stability_baseline_reproduced(result, directory):
    """Compare original metrics/state and every canonical record, including invalidity."""
    directory = Path(directory)
    expected = _read(directory / "summary.json")
    if (not isinstance(result.get("variant"), dict)
            or result["variant"] != expected.get("variant_terms")
            or result["variant"].get("key") != expected.get("variant")):
        raise ValueError("Original A6 actual variant terms differ")
    if not isinstance(result.get("records", {}).get("trades"), list):
        raise ValueError("Original A6 canonical trade records are required")
    trades = result["records"]["trades"]
    minimum_fraction = (sum(Decimal(str(row["commission"])) == Decimal("5")
        for row in trades) / len(trades) if trades else None)
    if (expected.get("minimum_commission_fill_fraction") != minimum_fraction
            or expected.get("score_quality_is_fixed_universe_diagnostic") is not True):
        raise ValueError("Original A6 derived summary annotations differ")
    annotations = {"variant", "variant_terms", "minimum_commission_fill_fraction", "score_quality_is_fixed_universe_diagnostic"}
    actual = {k: v for k, v in result.items() if k not in {"records", "final_state", "candidate", "variant"}}
    canonical = {k: v for k, v in expected.items() if k not in annotations}
    if actual != canonical or result["final_state"] != _read(directory / "final_state.json"):
        raise ValueError("Original A6 summary or final state failed exact reproduction")
    expected_names = {p.stem for p in directory.glob("*.csv")} | {p.stem for p in directory.glob("*.json") if p.stem in JSON_RECORDS}
    if set(result["records"]) != expected_names:
        raise ValueError("Original A6 canonical record types differ")
    proofs = {}
    for name, rows in result["records"].items():
        suffix = ".json" if name in JSON_RECORDS else ".csv"
        path = directory / (name + suffix)
        if name in JSON_RECORDS:
            if rows != json.loads(path.read_bytes()):
                raise ValueError("Original A6 JSON records differ: " + name)
        elif pd.DataFrame(rows).to_csv(index=False).encode() != path.read_bytes():
            raise ValueError("Original A6 CSV records differ: " + name)
        proofs[name] = stability._proof(path)
    return {"passed": True, "records": proofs,
        "comparison": "exact_original_summary_finalstate_all_canonical_JSON_CSV"}


def _write_result(result, directory, candidate):
    directory.mkdir(parents=True, exist_ok=False)
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            stability._save(directory / (name + ".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
    stability._save(directory / "final_state.json", result["final_state"])
    if "score_quality" in result:
        stability._save(directory / "score_quality.json", result["score_quality"])
    summary = {k: v for k, v in result.items() if k not in {"records", "final_state"}}
    summary["candidate"] = candidate
    stability._save(directory / "summary.json", summary)
    return summary


def _verify_execution(path):
    declaration = _read(path)
    if (declaration.get("kind") != EXECUTION_KIND or declaration.get("schema_version") != 1
            or declaration.get("phase") not in PHASES
            or declaration.get("models_verified_before_any_stage_return") is not True
            or declaration.get("active_paper_changed") is not False
            or declaration.get("broker_order_sent") is not False):
        raise ValueError("A complete unchanged stage execution declaration is required")
    protocol_path = declaration["training_declaration"]["path"]
    protocol = load_debug_declaration(protocol_path)
    stability._assert_proofs(declaration["bound_inputs"])
    if declaration["training_declaration"] != stability._proof(protocol_path):
        raise ValueError("Stage training declaration bytes changed")
    if declaration["execution"] != protocol["design"]["execution"]:
        raise ValueError("Fixed twenty-name execution mechanics changed")
    for k, proof in implementation_proofs().items():
        if declaration["bound_inputs"].get(k) != proof:
            raise ValueError("Stage execution implementation changed")
    manifest, model_proofs = _new_model_proofs(declaration["models_directory"], protocol_path, declaration["phase"])
    if (any(declaration["bound_inputs"].get(k) != value for k, value in model_proofs.items())
            or declaration["bound_inputs"].get("new_model_manifest") != stability._proof(Path(declaration["models_directory"]) / "manifest.json")):
        raise ValueError("Stage model directory differs from the exact frozen package")
    phase = declaration["phase"]
    if phase == "selection":
        if (declaration.get("selected_candidate") is not None
                or declaration.get("selection_models_directory") is not None
                or declaration.get("baseline_directory") is not None):
            raise ValueError("2023-only selection cannot consume later-stage configuration")
    else:
        decision = _verify_selection(declaration["bound_inputs"]["selection_decision"]["path"], protocol_path)
        if (declaration.get("selected_candidate") != decision["selected_candidate"]
                or manifest.get("models") != [decision["selected_candidate"]]
                or manifest.get("inputs", {}).get("selection_decision") != declaration["bound_inputs"]["selection_decision"]):
            raise ValueError("Later stage changed the original candidate selection")
        if phase == "confirmation":
            earlier = declaration.get("selection_models_directory")
            if earlier is None:
                raise ValueError("Confirmation is missing its original 2023 package")
            _, earlier_proofs = _new_model_proofs(earlier, protocol_path, "selection")
            if (any(declaration["bound_inputs"].get(k) != value for k, value in earlier_proofs.items())
                    or stability._proof(Path(earlier) / "manifest.json") != decision["selection_model_manifest"]
                    or declaration.get("baseline_directory") is not None):
                raise ValueError("Confirmation source packages differ from the selected 2023 model")
        else:
            if declaration.get("selection_models_directory") is not None or not declaration.get("baseline_directory"):
                raise ValueError("Retrospective baseline reference is required")
            baseline_proofs = _baseline_proofs(declaration["baseline_directory"])
            if any(declaration["bound_inputs"].get(k) != value for k, value in baseline_proofs.items()):
                raise ValueError("Retrospective reference directory differs from the frozen A6 source")
    return declaration, protocol


def run_debug_comparison(execution_declaration, output_directory, *, progress=None):
    """Run exactly 4/4/6 source-bound ledgers; no fit, search or live switch."""
    from stopat30m.ashare_model_debug_validation import load_debug_context, simulate_debug_model
    declaration, protocol = _verify_execution(execution_declaration)
    phase = declaration["phase"]
    output = _isolated_output(output_directory, protocol["active_paper_directory"])
    bound = {**declaration["bound_inputs"], "stage_execution_declaration": stability._proof(execution_declaration)}
    if declaration["expected_new_ledger_count"] != {"selection": 4, "confirmation": 4, "retrospective": 6}[phase]:
        raise ValueError("Stage ledger budget changed")
    output.mkdir(parents=True, exist_ok=False)
    (output / "declaration.json").write_bytes(Path(execution_declaration).read_bytes())
    archive = output / "source"; archive.mkdir()
    for name, proof in implementation_proofs().items():
        (archive / (name + "_" + Path(proof["path"]).name)).write_bytes(Path(proof["path"]).read_bytes())
    results, reproductions = [], {}
    try:
        family = "stability" if phase == "retrospective" else "primary"
        context = load_debug_context(declaration["training_declaration"]["path"], family)
        models = Path(declaration["models_directory"])
        selected = declaration["selected_candidate"]
        if phase == "selection":
            tasks = [("baseline", "2023", None)] + [(key, "2023", models) for key in CANDIDATES]
        elif phase == "confirmation":
            earlier = Path(declaration["selection_models_directory"])
            tasks = [("baseline", period, None) for period in ("2024", "continuous")]
            tasks += [(selected, "2024", models), (selected, "continuous", [earlier, models])]
        else:
            tasks = [("baseline", period, None) for period in ("2025", "2026", "continuous")]
            tasks += [(selected, period, models) for period in ("2025", "2026", "continuous")]
        with context["pool"].verified_batch():
            for candidate, period, package in tasks:
                if phase == "retrospective" and candidate != "baseline" and (set(reproductions) != {"2025", "2026", "continuous"}
                        or not all(r["passed"] for r in reproductions.values())):
                    raise ValueError("All three complete original baselines must reproduce before new retrospective returns")
                status = {"phase": phase, "candidate": candidate, "period": period, "stage": "costed_ledger"}
                stability._save(output / "status.json", status)
                if progress:
                    progress(status)
                result = simulate_debug_model(context, candidate=candidate, period=period, prediction_package=package)
                if candidate == "baseline":
                    # The original A6 orchestration adds this diagnostic after
                    # simulate_turnover. Preserve the same complete dictionary
                    # for exact summary replay without changing ledger records.
                    result["score_quality"] = stability.score_quality(context, model="LGB", period=period)
                if phase == "retrospective" and candidate == "baseline":
                    reproductions[period] = assert_debug_stability_baseline_reproduced(result,
                        Path(declaration["baseline_directory"]) / "hold10_cap2" / period)
                results.append(_write_result(result, output / candidate / period, candidate))
                if progress:
                    progress({**status, "completed": True, "valid": result["performance_valid"],
                        "net_return": result["metrics"]["total_return_lower_exact"],
                        "max_drawdown": result["metrics"]["max_drawdown_conservative_exact"]})
        stability._assert_proofs(bound)
        stability._assert_proofs(context["bound"])
        context["pool"].assert_unchanged(); context["actions"].assert_unchanged()
        _verify_execution(execution_declaration)
        if len(results) != declaration["expected_new_ledger_count"]:
            raise ValueError("Completed stage differs from the frozen ledger count")
        selection = decide_debug_selection(results) if phase == "selection" else None
        confirmation = decide_debug_confirmation(results, _read(bound["selection_decision"]["path"])) if phase == "confirmation" else None
        report = {"kind": "a14_model_debug_costed_comparison", "phase": phase,
            "completed_at": datetime.now(timezone.utc).isoformat(), "training_declaration": declaration["training_declaration"],
            "execution_declaration": bound["stage_execution_declaration"],
            "model_manifest": bound["new_model_manifest"], "bound_inputs": bound,
            "results": results, "selection": selection, "confirmation": confirmation,
            "original_stability_baseline_reproduction": reproductions,
            "ledger_count": len(results), "new_native_fits_here": 0,
            "all_performance_valid": all(r["performance_valid"] is True for r in results),
            "selected_candidate": selection["selected_candidate"] if selection else selected,
            "active_paper_changed": False, "broker_order_sent": False,
            "future_out_of_sample_claim": False, "automatic_model_promotion": False}
        stability._save(output / "report.json", report)
        if phase == "selection":
            decision = {"kind": DECISION_KIND, "schema_version": 1,
                "declared_at": datetime.now(timezone.utc).isoformat(),
                "selection_report": stability._proof(output / "report.json"),
                "selection_model_manifest": bound["new_model_manifest"],
                "training_declaration": declaration["training_declaration"], **selection}
            stability._save(output / "selection_decision.json", decision)
        stability._save(output / "status.json", {"phase": "completed", "stage": phase,
            "all_performance_valid": report["all_performance_valid"]})
        stability._save(output / "manifest.json", {"kind": report["kind"], "phase": phase,
            "bound_inputs": bound, "files": {str(p.relative_to(output)): stability.sha(p)
                for p in sorted(output.rglob("*")) if p.is_file()}})
        return report
    except Exception as exc:
        stability._save(output / "failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "phase": phase, "completed_ledgers": len(results), "reproduced_baselines": sorted(reproductions),
            "no_qualified_model_comparison_claim": True})
        stability._save(output / "status.json", {"phase": "failed", "stage": phase, "message": str(exc)})
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze-training")
    for name in ("primary_manifest", "stability_manifest", "design_path", "merger_actions", "active_paper", "output"):
        freeze.add_argument("--" + name.replace("_", "-"), required=True, type=Path)
    execution = commands.add_parser("freeze-execution")
    execution.add_argument("--declaration", required=True, type=Path)
    execution.add_argument("--models-directory", required=True, type=Path)
    execution.add_argument("--output", required=True, type=Path)
    execution.add_argument("--phase", choices=PHASES, required=True)
    execution.add_argument("--selection-path", type=Path)
    execution.add_argument("--selection-models-directory", type=Path)
    execution.add_argument("--baseline-directory", type=Path)
    run = commands.add_parser("run")
    run.add_argument("--declaration", required=True, type=Path)
    run.add_argument("--output-dir", required=True, type=Path)
    run.add_argument("--phase", choices=PHASES)
    args = parser.parse_args(argv)
    if args.command == "freeze-training":
        kwargs = vars(args).copy(); kwargs.pop("command")
        result = freeze_debug_declaration(**kwargs)
    elif args.command == "freeze-execution":
        result = freeze_debug_execution_declaration(args.declaration, args.models_directory, args.output,
            phase=args.phase, selection_path=args.selection_path,
            selection_models_directory=args.selection_models_directory, baseline_directory=args.baseline_directory)
    else:
        if args.phase is not None and _read(args.declaration).get("phase") != args.phase:
            parser.error("--phase must match the immutable execution declaration")
        result = run_debug_comparison(args.declaration, args.output_dir,
            progress=lambda row: print(json.dumps(row, ensure_ascii=False), flush=True))
    print(json.dumps({"kind": result["kind"], "phase": result.get("phase"),
        "all_performance_valid": result.get("all_performance_valid")}, ensure_ascii=False))
    return 0 if result.get("all_performance_valid", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
