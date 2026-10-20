"""Predeclared execution-only comparison of immutable LGB predictions.

Reproduce every original LGB ledger before running any of the four treatments.
This module never fits a model, regenerates scores or activates an account.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd

from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.ashare_stability_validation import (
    _assert_proofs, _proof, _save, load_stability_context, score_quality,
)
from stopat30m.ashare_turnover_validation import FIXED_VARIANTS, simulate_turnover
from stopat30m.data.qlib_snapshot import guard_qlib_output_path


PERIODS = ("2025", "2026", "continuous")
JSON_RECORDS = {"plans", "valuation_audit", "score_coverage", "turnover_plans", "holding_age", "holding_origin_actions"}


def _read(path):
    return json.loads(Path(path).read_text())


def _baseline_proofs(directory):
    directory = Path(directory).resolve()
    manifest = _read(directory / "manifest.json")
    if manifest.get("kind") != "a5_union_stability_costed_comparison":
        raise ValueError("A completed original stability comparison is required")
    proofs = {"baseline_manifest": _proof(directory / "manifest.json")}
    for name, digest in manifest["files"].items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory):
            raise ValueError("Baseline artifact escapes its directory")
        proofs["baseline:" + name] = {"path": str(path), "sha256": digest}
    _assert_proofs(proofs)
    report = _read(directory / "report.json")
    if not report.get("all_performance_valid"):
        raise ValueError("Original baseline contains invalid paths")
    return proofs


def implementation_proofs():
    from stopat30m import ashare_turnover_policy, ashare_turnover_validation
    return {name: _proof(path) for name, path in {
        "turnover_experiment": __file__,
        "turnover_policy": ashare_turnover_policy.__file__,
        "turnover_simulator": ashare_turnover_validation.__file__,
    }.items()}


def freeze_turnover_declaration(*, baseline_directory, source_bundle, protocol,
                                market_audit, predictions_directory,
                                merger_actions, design_freeze, output):
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve previous declarations")
    design = _read(design_freeze)
    if (design.get("any_candidate_returns_computed") is not False
            or design["design"].get("version") != 2
            or design["design"]["variants"] != list(FIXED_VARIANTS)
            or design["design"]["periods"] != list(PERIODS)):
        raise ValueError("The unchanged preimplementation five-variant design is required")
    original = {"design_file": design["bound_design"],
                "design_freeze": _proof(design_freeze),
                "original_predictions": design["existing_predictions"],
                "original_model_manifest": design["original_model_package_manifest"],
                "original_baseline_report": design["existing_baseline_report"],
                "original_baseline_manifest": design["existing_baseline_manifest"]}
    original.update({"native:" + key: proof for key, proof in design["existing_native_models"].items()})
    _assert_proofs(original)
    predictions = Path(predictions_directory).resolve()
    baseline = Path(baseline_directory).resolve()
    for actual, expected in ((predictions / "predictions.csv", design["existing_predictions"]),
                             (predictions / "manifest.json", design["original_model_package_manifest"]),
                             (baseline / "manifest.json", design["existing_baseline_manifest"]),
                             (baseline / "report.json", design["existing_baseline_report"])):
        if _proof(actual) != expected:
            raise ValueError("Execution comparison must reuse the original baseline and model package")
    receipt = verify_reused_stability_models(predictions, protocol, market_audit)
    bundle = _read(source_bundle)
    snapshot, scope = Path(bundle["snapshot_path"]), Path(bundle["scope_path"])
    baseline_report = _read(baseline / "report.json")
    for key, path in (("protocol", protocol), ("market_audit", market_audit),
                      ("predictions_manifest", predictions / "manifest.json"),
                      ("merger_actions", merger_actions)):
        if baseline_report["bound_inputs"][key]["sha256"] != _proof(path)["sha256"]:
            raise ValueError("Original baseline inputs changed: " + key)
    bound = {**original, **_baseline_proofs(baseline), **implementation_proofs(),
             "source_bundle": _proof(source_bundle), "protocol": _proof(protocol),
             "market_audit": _proof(market_audit), "merger_actions": _proof(merger_actions),
             "snapshot_manifest": _proof(snapshot / "manifest.json"),
             "scope_manifest": _proof(scope / "manifest.json")}
    _assert_proofs(bound)
    payload = {"kind": "a6_fixed_lgb_turnover_declaration", "version": 1,
        "declared_at": datetime.now(timezone.utc).isoformat(),
        "design": design["design"], "bound_inputs": bound,
        "baseline_directory": str(baseline), "snapshot": str(snapshot.resolve()),
        "scope_directory": str(scope.resolve()), "predictions_directory": str(predictions),
        "model_reuse_verification": receipt, "fitting_performed": False,
        "predictions_regenerated": False, "active_paper_or_broker_changed": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    _save(output, payload)
    return payload


def assert_full_baseline_reproduced(result, directory):
    """Exact summaries/final state plus every canonical original ledger record."""
    directory = Path(directory)
    expected = _read(directory / "summary.json")
    summary = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
    if summary != expected or result["final_state"] != _read(directory / "final_state.json"):
        raise ValueError("Baseline summary/final state differs; stop every treatment")
    expected_names = {path.stem for path in directory.glob("*.csv")} | {
        path.stem for path in directory.glob("*.json") if path.stem in JSON_RECORDS}
    if set(result["records"]) != expected_names:
        raise ValueError("Baseline canonical record types differ")
    proofs = {}
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            path = directory / (name + ".json")
            if rows != _read(path):
                raise ValueError("Baseline JSON ledger differs: " + name)
        else:
            path = directory / (name + ".csv")
            actual = pd.DataFrame(rows).to_csv(index=False).encode()
            if hashlib.sha256(actual).hexdigest() != _proof(path)["sha256"]:
                raise ValueError("Baseline CSV ledger differs: " + name)
        proofs[name] = _proof(path)
    return {"passed": True, "comparison": "exact all summaries, final state, canonical JSON and CSV bytes",
            "record_types": sorted(proofs), "canonical_records": proofs}


def _write_result(result, directory, variant):
    directory.mkdir(parents=True)
    for name, rows in result["records"].items():
        if name in JSON_RECORDS:
            _save(directory / (name + ".json"), rows)
        else:
            pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
    _save(directory / "final_state.json", result["final_state"])
    _save(directory / "score_quality.json", result["score_quality"])
    summary = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
    trades = pd.DataFrame(result["records"]["trades"])
    if len(trades):
        commissions = trades["commission"].map(Decimal)
        minimum_fraction = float((commissions == Decimal("5.00")).mean())
    else:
        minimum_fraction = None
    summary.update({"variant": variant["key"], "variant_terms": variant,
                    "minimum_commission_fill_fraction": minimum_fraction,
                    "score_quality_is_fixed_universe_diagnostic": True})
    _save(directory / "summary.json", summary)
    return summary


def decide_comparison(results):
    """Apply only the preregistered gates; never select by return alone."""
    paths = {(row["variant"], row["period"]): row for row in results}
    expected = {(v["key"], period) for v in FIXED_VARIANTS for period in PERIODS}
    if len(paths) != len(results) or set(paths) != expected:
        raise ValueError("Every predeclared path must be present exactly once")
    baseline = paths[("baseline", "continuous")]["metrics"]
    qualified, positive = [], []
    comparisons = []
    for variant in FIXED_VARIANTS[1:]:
        key = variant["key"]
        all_valid = all(paths[(key, p)]["performance_valid"] is True for p in PERIODS)
        m = paths[(key, "continuous")]["metrics"]
        improved = (all_valid and all(paths[("baseline", p)]["performance_valid"] is True for p in PERIODS)
            and Decimal(m["total_return_lower_exact"]) > Decimal(baseline["total_return_lower_exact"])
            and Decimal(m["max_drawdown_conservative_exact"]) >= Decimal(baseline["max_drawdown_conservative_exact"])
            and m["one_way_turnover_average_nav"] < baseline["one_way_turnover_average_nav"])
        positive_all = all_valid and all(Decimal(paths[(key, p)]["metrics"]["total_return_lower_exact"]) > 0 for p in PERIODS)
        if improved:
            qualified.append(key)
        if improved and positive_all:
            positive.append(key)
        comparisons.append({"variant": key, "all_three_paths_valid": all_valid,
                            "execution_improvement": improved, "positive_all_three_periods": positive_all})
    all_valid = all(row["performance_valid"] is True for row in results)
    return {"comparisons": comparisons, "execution_improvement_variants": qualified,
        "positive_stable_improvement_variants": positive,
        "next_research": ("resolve_invalid_paths" if not all_valid else
                          "defer_20_session_model_test_five_day_signal_forward" if positive else
                          "separate_20_session_label_research_justified"),
        "twenty_day_model_trained": False, "strategy_promoted": False,
        "historical_results_are_fresh_future_evidence": False}


def run_turnover_comparison(declaration, output_directory, *, progress=None):
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve every previous turnover comparison")
    declaration = Path(declaration).resolve()
    declaration_bytes = declaration.read_bytes()
    declaration_proof = {"path": str(declaration),
        "sha256": hashlib.sha256(declaration_bytes).hexdigest()}
    declared = json.loads(declaration_bytes)
    if (declared.get("kind") != "a6_fixed_lgb_turnover_declaration"
            or declared["design"]["variants"] != list(FIXED_VARIANTS)
            or declared["design"]["periods"] != list(PERIODS)):
        raise ValueError("A sealed fixed-turnover declaration is required")
    bound = {**declared["bound_inputs"], "runtime_declaration": declaration_proof}
    _assert_proofs(bound)
    if any(bound.get(name) != proof for name, proof in implementation_proofs().items()):
        raise ValueError("Turnover implementation changed after declaration")
    protocol, audit = bound["protocol"]["path"], bound["market_audit"]["path"]
    predictions = declared["predictions_directory"]
    model_receipt = verify_reused_stability_models(predictions, protocol, audit)
    _baseline_proofs(declared["baseline_directory"])
    output.mkdir(parents=True)
    _assert_proofs({"runtime_declaration": declaration_proof})
    (output / "declaration.json").write_bytes(declaration_bytes)
    _save(output / "status.json", {"phase": "loading_verified_fixed_inputs"})
    results, reproductions = [], {}
    try:
        context = load_stability_context(declared["snapshot"], declared["scope_directory"],
            protocol, audit, predictions, bound["merger_actions"]["path"])
        coverage_quality = {}
        with context["pool"].verified_batch():
            # Baseline is the first variant; no treatment is permitted until all
            # three original complete ledgers have reproduced exactly.
            for variant in FIXED_VARIANTS:
                if variant["key"] != "baseline" and len(reproductions) != len(PERIODS):
                    raise ValueError("Incomplete baseline reproduction; stop treatments")
                for period in PERIODS:
                    status = {"phase": "ledger", "variant": variant["key"], "period": period}
                    _save(output / "status.json", status)
                    if progress:
                        progress(status)
                    isolated = {key: value for key, value in context.items() if key != "score_coverage"}
                    result = simulate_turnover(isolated, period=period, variant=variant)
                    if variant["key"] == "baseline":
                        result["score_quality"] = score_quality(isolated, model="LGB", period=period)
                        reproductions[period] = assert_full_baseline_reproduced(result,
                            Path(declared["baseline_directory"]) / period / "LGB")
                        coverage_quality[period] = result["score_quality"]
                    else:
                        result["score_quality"] = coverage_quality[period]
                    results.append(_write_result(result, output / variant["key"] / period, variant))
                    if progress:
                        progress({**status, "completed": True, "valid": result["performance_valid"],
                            "net_return": result["metrics"]["total_return_lower_exact"]})
        _assert_proofs(bound)
        _assert_proofs(context["bound"])
        context["pool"].assert_unchanged()
        context["actions"].assert_unchanged()
        final_model_receipt = verify_reused_stability_models(predictions, protocol, audit)
        for name, proof in implementation_proofs().items():
            archived = output / "source" / Path(proof["path"]).name
            archived.parent.mkdir(exist_ok=True)
            shutil.copyfile(proof["path"], archived)
        report = {"kind": "a6_fixed_lgb_turnover_results", "completed_at": datetime.now(timezone.utc).isoformat(),
            "declaration": _proof(output / "declaration.json"), "results": results,
            "baseline_full_reproduction": reproductions,
            "all_performance_valid": all(row["performance_valid"] is True for row in results),
            "decision": decide_comparison(results), "bound_inputs": bound,
            "model_verification_before": model_receipt, "model_verification_after": final_model_receipt,
            "source_coverage": {key: context["market_audit"][key] for key in
                ("start", "end", "instrument_count", "raw_rows", "event_count")},
            "tax_classification_unresolved": context["market_audit"].get("tax_classification_unresolved", []),
            "fitting_performed": False, "predictions_regenerated": False,
            "active_paper_or_broker_changed": False,
            "limitations": ["Already examined retrospective historical comparison, not fresh future out-of-sample evidence.",
                "Daily bars and adverse costs do not establish queue priority or actual broker fills.",
                "Distribution tax uses conservative maximum reserve; unresolved source classifications remain disclosed.",
                "No automatic strategy promotion or twenty-session model fitting."]}
        _save(output / "report.json", report)
        _save(output / "status.json", {"phase": "completed", "all_performance_valid": report["all_performance_valid"]})
        _save(output / "manifest.json", {"kind": report["kind"], "files": {
            str(path.relative_to(output)): _proof(path)["sha256"]
            for path in sorted(output.rglob("*")) if path.is_file()}})
        return report
    except Exception as exc:
        _save(output / "failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
             "completed_paths": len(results), "baseline_reproduced_periods": sorted(reproductions),
             "no_valid_comparison_claim": True})
        _save(output / "status.json", {"phase": "failed", "message": str(exc)})
        raise


def register_commands(research_group):
    import click
    file = click.Path(exists=True, dir_okay=False, path_type=Path)
    directory = click.Path(exists=True, file_okay=False, path_type=Path)

    @research_group.command("ashare-turnover-freeze")
    @click.option("--baseline-directory", type=directory, required=True)
    @click.option("--source-bundle", type=file, required=True)
    @click.option("--protocol", type=file, required=True)
    @click.option("--market-audit", type=file, required=True)
    @click.option("--predictions-directory", type=directory, required=True)
    @click.option("--merger-actions", type=file, required=True)
    @click.option("--design-freeze", type=file, required=True)
    @click.option("--output", type=click.Path(path_type=Path), required=True)
    def freeze(**kwargs):
        """Seal the fixed scores, five variants, full baseline and implementation."""
        try:
            payload = freeze_turnover_declaration(**kwargs)
            click.echo(f"Turnover declaration sealed at {payload['declared_at']}")
        except (ValueError, OSError, KeyError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from exc

    @research_group.command("ashare-turnover-validate")
    @click.option("--declaration", type=file, required=True)
    @click.option("--output-dir", type=click.Path(path_type=Path), required=True)
    def validate(declaration, output_dir):
        """Reproduce all three baselines, then run the four predeclared variants."""
        try:
            report = run_turnover_comparison(declaration, output_dir,
                progress=lambda row: click.echo(json.dumps(row, ensure_ascii=False)))
            if report["all_performance_valid"] is not True:
                raise click.ClickException("Invalid paths; preserve diagnostics and do not select a strategy")
            click.echo(report["decision"]["next_research"])
        except (ValueError, OSError, KeyError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from exc
