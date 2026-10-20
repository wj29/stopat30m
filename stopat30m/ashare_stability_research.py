"""Separate2025/2026 stability generation with unchanged fixed fit mechanics.

The protocol/audit guards and publication entry point are intentionally separate
from completed2023/2024 artifacts. Alpha158 loading, observed row eligibility,
train-only processors and native LightGBM fitting use the same primary helpers.
"""
from __future__ import annotations
from dataclasses import asdict
from datetime import datetime, timezone
import gc
import json
from pathlib import Path
import shutil
import pandas as pd
from stopat30m import ashare_union_research as primary
from stopat30m.ashare_union_research import (
    LABEL, MARKET_CHECKS as PRIMARY_MARKET_CHECKS, _sha, _write, _raw_alpha158,
    _train_dataset, union_intervals, load_eligibility, dated_membership_mask,
    assert_market_originals_unchanged, _processor_payload, mature_signal_end,
    validate_training_rows, apply_frozen_processors, FixedQlibLGBSpec,
    fit_fixed_qlib_lightgbm, load_verified_historical_scope,
    guard_qlib_output_path, load_qlib_snapshot,
)

POLICY = "a5-stability-2025-2026-fixed-alpha158-annual-five-year-v1"
MARKET_CHECKS = (*PRIMARY_MARKET_CHECKS, "distribution_tax_accounting")


def implementation_sha256():
    return {**primary.implementation_sha256(), "stability_training": _sha(__file__)}


def _load_protocol(path, source, scope_directory):
    protocol = json.loads(Path(path).read_text())
    training = protocol.get("training", {})
    if (protocol.get("kind") != "a5_union_stability_fixed_research_protocol"
            or protocol.get("source_snapshot_manifest_sha256") != source.manifest_sha256
            or protocol.get("historical_scope_manifest_sha256") != _sha(Path(scope_directory) / "manifest.json")
            or training.get("execution_years") != [2025, 2026]
            or training.get("lookback_calendar_years") != 5 or training.get("purge_future_sessions") != 6
            or training.get("hyperparameter_search") is not False or training.get("early_stopping") is not False
            or training.get("label") != LABEL
            or training.get("fixed_lightgbm_spec") != asdict(FixedQlibLGBSpec())
            or training.get("membership_filter") != "signal_date_only_no_future_membership_survival_condition"):
        raise ValueError("A separately frozen fixed union research declaration is required")
    end = training.get("evaluation_end")
    if (not isinstance(end, str) or pd.Timestamp(end).date().isoformat() != end
            or pd.Timestamp(end).year != training["execution_years"][-1]):
        raise ValueError("Evaluation end must belong to the last declared execution year")
    return protocol, training


def union_fold_plan(calendar, years, *, evaluation_end):
    """Two separately declared 2025/2026 annual folds with five-year training windows."""
    days = pd.DatetimeIndex(pd.to_datetime(calendar))
    if (list(years) != [2025, 2026] or days.empty
            or days.has_duplicates or not days.is_monotonic_increasing or pd.Timestamp(evaluation_end) not in days
            or pd.Timestamp(evaluation_end).year != years[-1]):
        raise ValueError("Union folds require declared years and a unique observed evaluation end")
    result = []
    for year in years:
        prior = days[days < pd.Timestamp(f"{year}-01-01")]
        current = days[(days >= pd.Timestamp(f"{year}-01-01"))
            & (days <= min(pd.Timestamp(f"{year}-12-31"), pd.Timestamp(evaluation_end)))]
        if prior.empty or current.empty:
            raise ValueError("Missing actual preceding/current source sessions")
        cutoff = prior[-1].date().isoformat()
        result.append({"execution_year": year, "train_start": f"{year - 5}-01-01", "data_cutoff": cutoff,
            "train_signal_end": mature_signal_end(days, cutoff), "signal_start": cutoff,
            "signal_end": current[-1].date().isoformat(), "first_execution_session": current[0].date().isoformat()})
    return result


def validate_market_audit(audit, *, source_sha, scope_sha, stock_basic_sha, coverage_start, coverage_end):
    """A prior CSI300 audit or an unbound 'passed' flag cannot authorize fitting."""
    if (audit.get("kind") != "a5_stability_market_audit" or audit.get("status") != "passed"
            or audit.get("source_preflight_only") is not False
            or "distribution_tax_contracts" not in audit.get("bound_inputs", {})
            or audit.get("source_snapshot_manifest_sha256") != source_sha
            or audit.get("historical_scope_manifest_sha256") != scope_sha
            or audit.get("stock_basic_sha256") != stock_basic_sha
            or not isinstance(audit.get("start"), str) or not isinstance(audit.get("end"), str)
            or audit["start"] > coverage_start or audit["end"] < coverage_end
            or any(audit.get("checks", {}).get(check) is not True for check in MARKET_CHECKS)):
        raise ValueError("Complete verified, source-bound union market evidence is required before fitting")


def build_union_predictions(snapshot_path, scope_directory, stock_basic_path, protocol_path, market_audit_path,
                            output_dir, *, progress=None):
    """Fit fixed2025/2026 annual models only after their separate source gate."""
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Union training output exists; preserve original artifacts")
    source = load_qlib_snapshot(snapshot_path, verify=True)
    protocol, training = _load_protocol(protocol_path, source, scope_directory)
    calendar = pd.DatetimeIndex(pd.to_datetime((source.dataset_dir / "calendars/day.txt").read_text().splitlines()))
    folds = union_fold_plan(calendar, training["execution_years"], evaluation_end=training["evaluation_end"])
    audit = json.loads(Path(market_audit_path).read_text())
    validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=_sha(Path(scope_directory) / "manifest.json"), stock_basic_sha=_sha(stock_basic_path),
        coverage_start=folds[0]["first_execution_session"], coverage_end=training["evaluation_end"])
    assert_market_originals_unchanged(audit, stock_basic_path)
    inputs = {"declaration": {"path": str(Path(protocol_path).resolve()), "sha256": _sha(protocol_path)},
        "stock_basic": {"path": str(Path(stock_basic_path).resolve()), "sha256": _sha(stock_basic_path)},
        "market_audit": {"path": str(Path(market_audit_path).resolve()), "sha256": _sha(market_audit_path)}}
    implementation = implementation_sha256()
    if any(protocol.get("implementation_sha256", {}).get(name) != digest for name, digest in implementation.items()):
        raise ValueError("Union training implementation differs from its prior declaration")
    scope = load_verified_historical_scope(scope_directory, start=folds[0]["train_start"],
        end=training["evaluation_end"], require_official=True)
    sessions = [day.date().isoformat() for day in calendar if scope.start <= day.date().isoformat() <= scope.end]
    with scope.verified_batch():
        membership_audit = scope.audit_sessions(sessions)
        intervals = union_intervals(scope, scope.start, scope.end)
    if membership_audit["status"] != "passed":
        raise ValueError("Historical union membership audit must pass throughout training/inference")
    eligibility, source_files = load_eligibility(source, calendar, intervals, stock_basic_path)
    source_files["calendars/day.txt"] = source.manifest["files"]["calendars/day.txt"]["sha256"]
    target.mkdir(parents=True)
    shutil.copyfile(protocol_path, target / "declaration.json")
    shutil.copyfile(__file__, target / "union_training_implementation.py")
    _write(target / "membership_audit.json", membership_audit)
    all_scores, fold_records = [], []
    started = datetime.now(timezone.utc).isoformat()
    for fold in folds:
        year = fold["execution_year"]
        if progress:
            progress({"phase": "load_training_features", **fold})
        raw = _raw_alpha158(source, calendar, intervals, fold["train_start"], fold["data_cutoff"])
        keep, filtering = eligibility.mask(raw.index, intervals, cutoff=fold["data_cutoff"], labels=raw["label"].iloc[:, 0])
        filtered = raw.loc[keep].copy()
        del raw
        gc.collect()
        if filtered.empty:
            raise ValueError("No legal mature union training observations remain")
        dataset, handler = _train_dataset(filtered, fold)
        learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        maturity = validate_training_rows(learned, fold, calendar)
        if not dated_membership_mask(learned.index, intervals).all():
            raise ValueError("An out-of-signal-membership row reached model fitting")
        if progress:
            progress({"phase": "fit", "execution_year": year, **maturity, "filtering": filtering})
        model = fit_fixed_qlib_lightgbm(dataset, spec=FixedQlibLGBSpec())
        processors = _processor_payload(handler, model.feature_names)
        folder = target / f"LGB_{year}"
        folder.mkdir()
        fitted_at = datetime.now(timezone.utc).isoformat()
        model.save_native(folder / "model", metadata={"model": "LGB", "execution_year": year,
            "fitted_inference_processors": processors["processors"], "declaration_sha256": inputs["declaration"]["sha256"],
            "fit_role": "retrospective_reconstructed_history", "train_signal_end": fold["train_signal_end"]})
        _write(folder / "processors.json", processors)
        record = {**fold, **maturity, "training_filtering": filtering, "fitted_at": fitted_at,
            "model_sha256": _sha(folder / "model/model.txt"), "historical_model_freeze_claim": False}
        del filtered, learned, dataset, handler
        gc.collect()
        raw = _raw_alpha158(source, calendar, intervals, fold["signal_start"], fold["signal_end"])
        keep, inference_filter = eligibility.mask(raw.index, intervals)
        features = raw.loc[keep, "feature"].copy()
        del raw
        gc.collect()
        scores = apply_frozen_processors(features, model, processors["processors"]).rename("score").reset_index()
        scores["datetime"] = pd.to_datetime(scores["datetime"]).dt.strftime("%Y-%m-%d")
        scores["model"], scores["execution_year"] = "LGB", year
        scores = scores[["model", "execution_year", "datetime", "instrument", "score"]]
        scores.to_csv(folder / "predictions.csv", index=False, float_format="%.17g")
        all_scores.append(scores)
        record["inference_filtering"] = inference_filter
        _write(folder / "protocol.json", record)
        _write(folder / "manifest.json", {"schema_version": 1,
            "files": {str(file.relative_to(folder)): _sha(file) for file in sorted(folder.rglob("*")) if file.is_file()}})
        fold_records.append(record)
        if progress:
            progress({"phase": "fold_complete", "execution_year": year, "prediction_rows": len(scores), **maturity})
        del features, model, scores
        gc.collect()
    predictions = pd.concat(all_scores, ignore_index=True).sort_values(["model", "execution_year", "datetime", "instrument"])
    if predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
        raise ValueError("Duplicate union scores within an execution-year fold")
    scope.assert_unchanged()
    if any(_sha(item["path"]) != item["sha256"] for item in inputs.values()):
        raise ValueError("Declared source inputs changed while training")
    if any(_sha(source.dataset_dir / name) != digest for name, digest in source_files.items()):
        raise ValueError("Model source feature bytes changed while training")
    if implementation_sha256() != implementation:
        raise ValueError("Declared implementation bytes changed while training")
    assert_market_originals_unchanged(audit, stock_basic_path)
    predictions.to_csv(target / "predictions.csv", index=False, float_format="%.17g")
    _write(target / "source_files.json", source_files)
    manifest = {"schema_version": 1, "kind": "a5_union_stability_fixed_predictions", "policy": POLICY,
        "started_at": started, "completed_at": datetime.now(timezone.utc).isoformat(), "inputs": inputs,
        "source_snapshot": str(source.path), "source_manifest_sha256": source.manifest_sha256,
        "scope_sources": scope.source_receipts(), "folds": fold_records, "models": ["LGB"],
        "prediction_rows": len(predictions), "hyperparameter_search": False, "early_stopping": False,
        "historical_model_freeze_claim": False, "new_future_out_of_sample_claim": False,
        "active_paper_changed": False, "selection_performed": False,
        "implementation_sha256": implementation,
        "files": {str(file.relative_to(target)): _sha(file) for file in sorted(target.rglob("*")) if file.is_file()}}
    _write(target / "manifest.json", manifest)
    return {"manifest": manifest, "predictions": predictions, "path": target}
