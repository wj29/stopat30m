"""Predeclared5/10/20-session label diagnostics on immutable LGB scores.

No model fitting, score regeneration, portfolio simulation or selection occurs.
Observed adjusted price labels are not executable or after-tax portfolio returns.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot


HORIZONS = (5, 10, 20)
PERIODS = ("2025", "2026", "continuous")
DESIGN_TERMS = {
    "kind": "a7_frozen_lgb_horizon_diagnostic_design", "version": 1,
    "horizons_actual_sessions": list(HORIZONS), "periods": list(PERIODS),
    "evaluation_end": "2026-10-19", "entry": "observed normalized open at t+1",
    "exit": "observed normalized open at t+horizon+1", "training_or_new_predictions": False,
    "original_five_session_guard": "full original score_quality dict must exactly reproduce all3 periods before other horizon output",
    "native_comparison": "each horizon consumes only its own fully mature observed positive price pairs; explicitly report excluded pairs and days",
    "matched_comparison": "all horizons share the same t+21 mature signal dates and intersection of valid names across5/10/20 endpoints within signal-day historical members; never require future index membership",
    "statistics": ["IC", "rank_IC", "positive_date_fraction", "pair_coverage", "daily_observations"],
    "inference": "rank diagnostic only; does not establish executable returns or select strategies or model hyperparameters",
    "input_score_is_expected_return": False, "strategy_or_paper_changes": False,
    "real_horizon_results_computed_before_design": False,
}


def _read(path):
    return json.loads(Path(path).read_bytes())


def _label(horizon):
    if type(horizon) is not int or horizon not in HORIZONS:
        raise ValueError("Only the predeclared5/10/20-session horizons are allowed")
    return f"Ref($open,-{horizon+1})/Ref($open,-1)-1"


def _sessions(context, period):
    if period not in PERIODS or period not in context["protocol"]["periods"]:
        raise ValueError("A predeclared diagnostic period is required")
    start, end = context["protocol"]["periods"][period]
    calendar = context["calendar"]
    if not calendar or calendar != sorted(set(calendar)):
        raise ValueError("A full ordered actual exchange calendar is required")
    final = [number for number, day in enumerate(calendar) if day <= end]
    if not final:
        raise ValueError("No actual diagnostic calendar observation")
    days = [(number-1, calendar[number-1]) for number, day in enumerate(calendar)
        if number > 0 and start <= day <= end]
    if not days:
        raise ValueError("No actual execution sessions and preceding signals")
    return days, final[-1]


def _names(context, signal_day):
    try:
        scores = context["scores"]["LGB"].xs(pd.Timestamp(signal_day), level="datetime")
    except KeyError:
        raise ValueError("Frozen predictions lack a declared prior-close signal") from None
    if scores.index.has_duplicates:
        raise ValueError("Duplicate frozen score identity")
    members = context["pool"].members_on(signal_day)
    names = [inst for inst in members if inst in scores and np.isfinite(scores.loc[inst])
        and inst in context["normalized_open"]]
    return names, scores, len(members)


def _empty_exclusions():
    return {"immature_signal_days": 0, "missing_label_pairs": 0,
        "insufficient_cross_section_days": 0, "constant_score_or_label_days": 0}


def _summary(rows, excluded, horizon):
    summary = {"label": _label(horizon), "minimum_cross_section": 20,
        "daily_observations": len(rows), "excluded": excluded,
        "future_membership_filter_used": False,
        "quality_diagnostic_does_not_filter_actual_orders": True}
    for field in ("IC", "rank_IC"):
        values = np.asarray([row[field] for row in rows])
        summary[field+"_mean"] = float(values.mean()) if len(values) else None
        summary[field+"_std"] = float(values.std(ddof=0)) if len(values) else None
        summary[field+"_positive_fraction"] = float((values > 0).mean()) if len(values) else None
    return {"summary": summary, "daily": rows}


def _correlation(scores, labels):
    predicted = scores.loc[labels.index].astype(float)
    if labels.nunique() < 2 or predicted.nunique() < 2:
        return None
    ic, rank_ic = predicted.corr(labels), predicted.rank().corr(labels.rank())
    if not np.isfinite(ic) or not np.isfinite(rank_ic):
        raise ValueError("Nonfinite cross-sectional horizon diagnostic")
    return float(ic), float(rank_ic)


def _native_quality(context, *, period, horizon):
    """Preserve the original five-session calculation's full dictionary exactly."""
    _label(horizon)
    calendar, opening = context["calendar"], context["normalized_open"]
    days, last = _sessions(context, period)
    rows, excluded, coverage = [], _empty_exclusions(), []
    for location, signal_day in days:
        if location+horizon+1 > last:
            excluded["immature_signal_days"] += 1
            coverage.append({"signal_date": signal_day, "status": "immature_label",
                "entry_delay_sessions": 1, "future_dependency_sessions": horizon+1})
            continue
        names, scores, member_count = _names(context, signal_day)
        left, right = opening.loc[calendar[location+1], names], opening.loc[calendar[location+horizon+1], names]
        observed = np.isfinite(left) & left.gt(0) & np.isfinite(right) & right.gt(0)
        missing = int((~observed).sum())
        excluded["missing_label_pairs"] += missing
        names = list(observed.index[observed])
        detail = {"signal_date": signal_day, "entry_date": calendar[location+1],
            "label_end": calendar[location+horizon+1], "signal_historical_members": member_count,
            "finite_score_and_price_column_pairs": len(observed), "missing_label_pairs": missing,
            "missing_finite_score_or_model_price_column": member_count-len(observed),
            "observed_label_pairs": len(names), "status": "included"}
        coverage.append(detail)
        if len(names) < 20:
            excluded["insufficient_cross_section_days"] += 1
            detail["status"] = "insufficient_cross_section"
            continue
        labels = right.loc[names]/left.loc[names]-1
        correlation = _correlation(scores, labels)
        if correlation is None:
            excluded["constant_score_or_label_days"] += 1
            detail["status"] = "constant_score_or_label"
            continue
        rows.append({"signal_date": signal_day, "label_end": calendar[location+horizon+1],
            "pairs": len(names), "IC": correlation[0], "rank_IC": correlation[1]})
    return _summary(rows, excluded, horizon), coverage


def _matched_quality(context, *, period):
    """Paired dates and exact stock intersection, using only signal membership."""
    calendar, opening = context["calendar"], context["normalized_open"]
    days, last = _sessions(context, period)
    rows = {h: [] for h in HORIZONS}
    excluded, coverage = _empty_exclusions(), []
    for location, signal_day in days:
        if location+21 > last:
            excluded["immature_signal_days"] += 1
            coverage.append({"signal_date": signal_day, "status": "immature_common_t_plus_21_label"})
            continue
        names, scores, member_count = _names(context, signal_day)
        left = opening.loc[calendar[location+1], names]
        right = {h: opening.loc[calendar[location+h+1], names] for h in HORIZONS}
        available = {h: np.isfinite(left) & left.gt(0) & np.isfinite(right[h]) & right[h].gt(0) for h in HORIZONS}
        common_mask = available[5] & available[10] & available[20]
        common = list(common_mask.index[common_mask])
        excluded["missing_label_pairs"] += int((~common_mask).sum())
        detail = {"signal_date": signal_day, "entry_date": calendar[location+1],
            "latest_required_label_end": calendar[location+21], "signal_historical_members": member_count,
            "finite_score_and_price_column_pairs": len(names), "common_observed_pairs": len(common),
            "missing_finite_score_or_model_price_column": member_count-len(names),
            "missing_any_endpoint_pairs": int((~common_mask).sum()),
            "missing_label_pairs_by_horizon": {str(h): int((~available[h]).sum()) for h in HORIZONS},
            "common_instruments": common, "status": "included"}
        coverage.append(detail)
        if len(common) < 20:
            excluded["insufficient_cross_section_days"] += 1
            detail["status"] = "insufficient_common_cross_section"
            continue
        correlations = {h: _correlation(scores, right[h].loc[common]/left.loc[common]-1) for h in HORIZONS}
        constant = [h for h in HORIZONS if correlations[h] is None]
        if constant:
            # A paired comparison cannot silently retain different dates after
            # one horizon's correlation becomes undefined on the same names.
            excluded["constant_score_or_label_days"] += 1
            detail.update(status="constant_score_or_label_in_common_comparison", constant_horizons=constant)
            continue
        for h in HORIZONS:
            rows[h].append({"signal_date": signal_day, "label_end": calendar[location+h+1],
                "pairs": len(common), "IC": correlations[h][0], "rank_IC": correlations[h][1]})
    return {str(h): _summary(rows[h], deepcopy(excluded), h) for h in HORIZONS}, coverage


def diagnose_frozen_horizons(context, *, expected_five_day=None):
    """Compute all nine native and nine matched sets after all three5-day guards."""
    if set(context["protocol"]["periods"]) != set(PERIODS):
        raise ValueError("All three predeclared periods are required")
    native_five, coverage_five = {}, {}
    for period in PERIODS:
        native_five[period], coverage_five[period] = _native_quality(context, period=period, horizon=5)
        original = stability.score_quality(context, model="LGB", period=period)
        if native_five[period] != original or expected_five_day is not None and native_five[period] != expected_five_day[period]:
            raise ValueError("Original full five-session score_quality failed exact reproduction; stop every horizon output")
    result = {}
    for period in PERIODS:
        native, coverage = {"5": native_five[period]}, {"5": coverage_five[period]}
        for h in (10, 20):
            native[str(h)], coverage[str(h)] = _native_quality(context, period=period, horizon=h)
        matched, matched_coverage = _matched_quality(context, period=period)
        result[period] = {"native": native, "native_coverage": coverage,
            "matched": matched, "matched_coverage": matched_coverage,
            "all_three_original_five_session_dicts_reproduced_before_other_horizons": True,
            "matched_date_policy": "common_t_plus_21_maturity_and_nonconstant_cross_sections_for_all_horizons",
            "matched_name_policy": "intersection_of_all_observed_positive_endpoints_among_signal_date_members_with_finite_scores",
            "future_membership_conditioned": False}
    return result


def _verify_design(design_freeze):
    payload = _read(design_freeze)
    if (payload.get("kind") != "a7_preimplementation_horizon_design_freeze"
            or payload.get("no_new_fit_or_parameters_or_forward_strategy_changes") is not True
            or set(payload.get("design", {})) != set(DESIGN_TERMS) | {"declared_at"}
            or any(payload["design"].get(key) != value for key, value in DESIGN_TERMS.items())
            or set(payload.get("original_baseline_scores", {})) != set(PERIODS)):
        raise ValueError("The complete unchanged preregistered horizon design is required")
    proofs = {key: payload[key] for key in ("bound_design", "original_predictions", "original_models_manifest", "original_turnover_report")}
    proofs.update({"baseline_score:"+key: value for key, value in payload["original_baseline_scores"].items()})
    stability._assert_proofs(proofs)
    if _read(payload["bound_design"]["path"]) != payload["design"]:
        raise ValueError("Design freeze differs from its bound original design")
    return payload, proofs


def run_frozen_horizon_diagnostics(design_freeze, output_dir, *, progress=None):
    """Seal source/model/code before calculating diagnostic correlations."""
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve every previous diagnostic output")
    frozen, bound = _verify_design(design_freeze)
    source_report = _read(frozen["original_turnover_report"]["path"])
    if source_report.get("kind") != "a6_fixed_lgb_turnover_results":
        raise ValueError("The source must be the completed immutable turnover comparison")
    for name, original in source_report["bound_inputs"].items():
        if name in bound and bound[name] != original:
            raise ValueError("Original source proof conflicts with the frozen design: "+name)
        bound[name] = original
    bound.update({"horizon_design_freeze": stability._proof(design_freeze),
        "horizon_implementation": stability._proof(__file__),
        "model_reuse_verifier": stability._proof(verify_reused_stability_models.__code__.co_filename)})
    predictions = Path(frozen["original_models_manifest"]["path"]).parent
    if stability._proof(predictions/"predictions.csv") != frozen["original_predictions"]:
        raise ValueError("The diagnostic cannot replace original frozen predictions")
    model_manifest = _read(predictions/"manifest.json")
    for name, digest in model_manifest["files"].items():
        artifact = (predictions/name).resolve()
        if not artifact.is_relative_to(predictions.resolve()):
            raise ValueError("Frozen model artifact escapes its package")
        bound["frozen_model:"+name] = {"path": str(artifact), "sha256": digest}
    bound.update({"current_execution:"+name: value for name, value in stability.implementation_proofs().items()})
    stability._assert_proofs(bound)
    bundle = _read(bound["source_bundle"]["path"])
    protocol, audit = bound["protocol"]["path"], bound["market_audit"]["path"]
    reuse = verify_reused_stability_models(predictions, protocol, audit)
    expected = {period: _read(frozen["original_baseline_scores"][period]["path"]) for period in PERIODS}
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(__file__, output/"diagnostic_implementation.py")
    bound["archived_horizon_implementation"] = stability._proof(output/"diagnostic_implementation.py")
    declaration = {"kind": "a7_frozen_lgb_horizon_runtime_declaration", "declared_at": datetime.now(timezone.utc).isoformat(),
        "design": frozen["design"], "bound_inputs": bound, "model_reuse_verification": reuse,
        "any_horizon_results_computed_before_runtime_declaration": False,
        "fitting_performed": False, "predictions_regenerated": False, "strategy_or_paper_changed": False}
    stability._save(output/"declaration.json", declaration)
    declaration_proof = stability._proof(output/"declaration.json")
    try:
        if progress:
            progress({"phase": "runtime_declaration_sealed", "declaration": declaration_proof})
        context = stability.load_stability_context(bundle["snapshot_path"], bundle["scope_path"], protocol, audit,
            predictions, bound["merger_actions"]["path"])
        with context["pool"].verified_batch():
            diagnostics = diagnose_frozen_horizons(context, expected_five_day=expected)
        stability._assert_proofs(bound)
        stability._assert_proofs(context["bound"])
        context["pool"].assert_unchanged(); context["actions"].assert_unchanged()
        load_qlib_snapshot(bundle["snapshot_path"], verify=True)
        verify_reused_stability_models(predictions, protocol, audit)
        stability._assert_proofs({"runtime_declaration": declaration_proof})
        rows = []
        for period in PERIODS:
            for mode in ("native", "matched"):
                for horizon in HORIZONS:
                    item = diagnostics[period][mode][str(horizon)]
                    stability._save(output/f"{period}_{mode}_h{horizon}.json", item)
                    summary = item["summary"]
                    rows.append({"period": period, "mode": mode, "horizon_actual_sessions": horizon,
                        "entry_delay_sessions": 1, "future_dependency_sessions": horizon+1,
                        "daily_observations": summary["daily_observations"],
                        **{key: summary[key] for key in ("IC_mean", "IC_std", "IC_positive_fraction",
                            "rank_IC_mean", "rank_IC_std", "rank_IC_positive_fraction")},
                        **summary["excluded"]})
            stability._save(output/f"{period}_coverage.json", {key: value for key, value in diagnostics[period].items()
                if key not in {"native", "matched"}})
        pd.DataFrame(rows).to_csv(output/"comparison.csv", index=False)
        report = {"kind": "a7_frozen_lgb_horizon_diagnostics", "completed_at": datetime.now(timezone.utc).isoformat(),
            "declaration": declaration_proof, "all_three_original_five_session_dicts_reproduced": True,
            "all_nine_native_and_nine_matched_sets_present": True, "results": diagnostics,
            "bound_inputs": {**bound, **{"consumed_context:"+key: value for key, value in context["bound"].items()}},
            "fitting_performed": False, "predictions_regenerated": False,
            "strategy_or_paper_changed": False, "parameters_selected": False,
            "limitations": ["Previously examined historical windows, not new future out-of-sample evidence.",
                "These are adjusted-open label correlations, not executable or after-tax portfolio returns.",
                "Native horizon populations differ; matched sets explicitly use common dates and observed endpoint names.",
                "Missing/delisted/identity endpoint exclusions are disclosed and may induce survivorship in labelled diagnostics.",
                "A portfolio's unresolved terminal entitlement does not invalidate independently observed mature labels for other names.",
                "Overlapping multi-session labels make daily correlations dependent; no independent-day significance claim is made.",
                "CSRankNorm training scores are rank-oriented values, not calibrated expected returns or portfolio neutrality."]}
        stability._save(output/"report.json", report)
        stability._assert_proofs(bound)
        stability._assert_proofs(context["bound"])
        stability._save(output/"manifest.json", {"kind": report["kind"], "bound_inputs": report["bound_inputs"],
            "files": {str(path.relative_to(output)): stability.sha(path) for path in sorted(output.rglob("*")) if path.is_file()}})
        return report
    except Exception as exc:
        stability._save(output/"failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "no_completed_horizon_diagnostic_claim": True})
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--design-freeze", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    result = run_frozen_horizon_diagnostics(args.design_freeze, args.output_dir,
        progress=lambda row: print(json.dumps(row, ensure_ascii=False), flush=True))
    print(json.dumps({"output": str(args.output_dir.resolve()),
        "all_three_original_five_session_dicts_reproduced": result["all_three_original_five_session_dicts_reproduced"],
        "fitting_performed": False, "parameters_selected": False}))


if __name__ == "__main__":
    main()
