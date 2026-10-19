"""One declared annual rolling Alpha158 model, fitted retrospectively today.

This is research output only. It neither selects a winner nor changes an
existing forward paper model, and does not claim historic model freeze times.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import gc
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import PackageHistoricalPool, REQUIRED_FIELDS
from stopat30m.ashare_forward import LABEL, _processor_payload, _training_dataset, mature_signal_end
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm, load_fixed_qlib_model

POLICY = "a3-annual-five-year-alpha158-retrospective-v1"


def _sha(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


def annual_fold_plan(calendar, years, *, evaluation_end):
    """Use actual source sessions, including the prior-year close signal."""
    dates = pd.DatetimeIndex(pd.to_datetime(calendar))
    if dates.has_duplicates or not dates.is_monotonic_increasing or pd.Timestamp(evaluation_end) not in dates:
        raise ValueError("Rolling folds require a unique actual-session calendar and observed evaluation end")
    if list(years) != [2023, 2024, 2025, 2026]:
        raise ValueError("Only the four predeclared annual folds are authorized")
    result = []
    for year in years:
        prior = dates[dates < pd.Timestamp(f"{year}-01-01")]
        current = dates[(dates >= pd.Timestamp(f"{year}-01-01")) & (dates <= min(pd.Timestamp(f"{year}-12-31"), pd.Timestamp(evaluation_end)))]
        if prior.empty or current.empty:
            raise ValueError("An annual fold has no actual preceding/current source sessions")
        cutoff = prior[-1].date().isoformat()
        result.append({"execution_year": year, "train_start": f"{year - 5}-01-01", "data_cutoff": cutoff,
                       "train_signal_end": mature_signal_end(dates, cutoff), "signal_start": cutoff,
                       "signal_end": current[-1].date().isoformat(), "first_execution_session": current[0].date().isoformat()})
    return result


def apply_frozen_processors(features, model, processors):
    """Apply saved train statistics in original feature order; never fit."""
    if (len(model.feature_names) != 158 or list(features.columns) != list(model.feature_names)
            or features.index.has_duplicates or [item.get("class") for item in processors] != ["RobustZScoreNorm", "Fillna"]):
        raise ValueError("Frozen Alpha158 feature order/index/processor schema differs")
    norm = processors[0]
    mean = np.asarray(norm.get("mean_train"), dtype=float)
    std = np.asarray(norm.get("std_train"), dtype=float)
    if mean.shape != (158,) or std.shape != (158,) or not np.isfinite(mean).all() or not np.isfinite(std).all() or (std <= 0).any() or not norm.get("clip_outlier"):
        raise ValueError("Frozen train normalizer is incomplete")
    values = features.apply(pd.to_numeric, errors="raise")
    if np.isinf(values.to_numpy(dtype=float)).any():
        raise ValueError("Infinite raw inference features are invalid observations")
    # The official Qlib processor subtracts and divides in separate in-place
    # operations. Its raw feature frame and fitted median/MAD are float32.
    # Promoting this calculation to float64 changes tree-threshold decisions.
    # Preserve each feature's source dtype and the original intermediate round.
    normalized = values.copy()
    normalized -= mean.astype(normalized.to_numpy().dtype)
    normalized /= std.astype(normalized.to_numpy().dtype)
    normalized = normalized.clip(-3, 3).fillna(0)
    scores = model.booster.predict(normalized, num_threads=1)
    if not np.isfinite(scores).all():
        raise ValueError("Rolling model returned nonfinite scores")
    return pd.Series(scores, index=features.index, name="score")


def validate_training_rows(learned, fold, calendar):
    """Prove every fitting signal's complete label ends at/before cutoff."""
    if learned.empty or not isinstance(learned.index, pd.MultiIndex) or "datetime" not in learned.index.names:
        raise ValueError("Rolling training needs nonempty dated learning observations")
    dates = pd.DatetimeIndex(learned.index.get_level_values("datetime"))
    source = pd.DatetimeIndex(pd.to_datetime(calendar))
    if (dates.min() < pd.Timestamp(fold["train_start"]) or dates.max() > pd.Timestamp(fold["train_signal_end"])
            or not dates.isin(source).all()):
        raise ValueError("Immature or out-of-fold rows reached rolling model fitting")
    last = source.get_loc(dates.max()) + 6
    if last >= len(source) or source[last] > pd.Timestamp(fold["data_cutoff"]):
        raise ValueError("A rolling training label crosses the prior-year cutoff")
    return {"training_rows": len(learned), "first_training_signal": dates.min().date().isoformat(),
            "last_training_signal": dates.max().date().isoformat(), "last_label_execution_end": source[last].date().isoformat()}


def _context(snapshot, calendar, fold):
    pool = PackageHistoricalPool(snapshot.dataset_dir / "instruments/csi300.txt", start=fold["train_start"], end=fold["data_cutoff"])
    instruments = pool.qlib_intervals(fold["train_start"], fold["data_cutoff"])
    files = {"calendars/day.txt", "instruments/csi300.txt"}
    for symbol in instruments:
        files.update(f"features/{symbol.lower()}/{field}.day.bin" for field in REQUIRED_FIELDS)
    return {"snapshot": snapshot, "dataset": snapshot.dataset_dir, "calendar": calendar, "pool": pool,
            "instruments": instruments, "mature_end": fold["train_signal_end"],
            "used_files": {name: snapshot.manifest["files"][name]["sha256"] for name in sorted(files)}}


def _raw_features(snapshot, calendar, fold):
    import qlib
    from qlib.contrib.data.handler import Alpha158
    from qlib.data.dataset import DatasetH
    start, end = fold["signal_start"], fold["signal_end"]
    location = calendar.get_loc(pd.Timestamp(start))
    if location < 80:
        raise ValueError("Insufficient actual Alpha158 warmup sessions")
    warmup = calendar[location - 80].date().isoformat()
    pool = PackageHistoricalPool(snapshot.dataset_dir / "instruments/csi300.txt", start=start, end=end)
    instruments = pool.qlib_intervals(start, end)
    qlib.init(provider_uri=str(snapshot.dataset_dir), region="cn", kernels=1, expression_cache=None, dataset_cache=None)
    handler = Alpha158(instruments=instruments, start_time=warmup, end_time=end,
                       infer_processors=[], learn_processors=[], label=[LABEL])
    dataset = DatasetH(handler=handler, segments={"predict": (start, end)})
    features = dataset.prepare("predict", col_set="feature", data_key="infer")
    if isinstance(features.columns, pd.MultiIndex):
        features = features["feature"]
    days = pd.DatetimeIndex(features.index.get_level_values("datetime"))
    if features.empty or days.min() < pd.Timestamp(start) or days.max() > pd.Timestamp(end):
        raise ValueError("Raw inference features exceed the declared annual signal span")
    return features, sorted(instruments)


def _verify_baseline(path, snapshot):
    root = Path(path).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("snapshot_manifest_sha256") != snapshot.manifest_sha256:
        raise ValueError("M0 and rolling models must use exactly the same repaired source snapshot")
    for name, digest in manifest["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or _sha(root / relative) != digest:
            raise ValueError("Frozen baseline result checksum changed")
    model = load_fixed_qlib_model(root / "model")
    if model.spec != FixedQlibLGBSpec() or "fitted_inference_processors" not in model.metadata:
        raise ValueError("Baseline model is not the unchanged declared native model")
    protocol = json.loads((root / "protocol.json").read_text())
    if protocol["config"]["train_start"] != "2012-01-01" or protocol["config"]["train_end"] != "2022-12-31":
        raise ValueError("M0 training interval differs from the declared 2012-2022 baseline")
    return model


def _load_spec(path):
    spec = json.loads(Path(path).read_text())
    models = {model["id"]: model for model in spec["models"]}
    rolling = models["M1"]
    if (set(models) != {"M0", "M1"} or models["M0"].get("refit") is not False
            or rolling.get("policy") != "annual_rolling_five_calendar_years" or rolling.get("execution_years") != [2023, 2024, 2025, 2026]
            or rolling.get("purge_future_sessions") != 6 or rolling.get("hyperparameter_search") is not False
            or rolling.get("early_stopping") is not False or not rolling.get("same_alpha158_and_fixed_lightgbm_params")):
        raise ValueError("Only the frozen M0 plus one fixed rolling M1 declaration is authorized")
    return spec, models


def build_research_predictions(snapshot, baseline_origin, output_dir, spec_path, *, progress=None, reuse_fitted_dir=None):
    """Fit four fixed folds and reuse M0; bind all outputs without selection."""
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Rolling research output exists; preserve the original artifact")
    spec, models = _load_spec(spec_path)
    declared_sha = _sha(spec_path)
    source = load_qlib_snapshot(snapshot, verify=True)
    if source.manifest_sha256 != spec["source_manifest_sha256"]:
        raise ValueError("Research source differs from the predeclared repaired vintage")
    origin = Path(baseline_origin).resolve()
    if origin != Path(models["M0"]["training_origin"]).resolve():
        raise ValueError("M0 origin differs from the frozen optimization declaration")
    baseline = _verify_baseline(origin, source)
    baseline_manifest_sha = _sha(origin / "manifest.json")
    baseline_model_sha = _sha(origin / "model/model.txt")
    reused = Path(reuse_fitted_dir).resolve() if reuse_fitted_dir else None
    reuse_receipt = None
    if reused:
        original_implementation = reused / "rolling_implementation_original.py"
        if not original_implementation.is_file():
            raise ValueError("Reused models require their original fit implementation bytes")
        reuse_receipt = {"path": str(reused), "original_fit_implementation_sha256": _sha(original_implementation),
                         "reason": "Preserve fitted native models while correcting saved-processor inference dtype"}
    calendar = pd.DatetimeIndex(pd.to_datetime((source.dataset_dir / "calendars/day.txt").read_text().splitlines()))
    folds = annual_fold_plan(calendar, models["M1"]["execution_years"], evaluation_end=spec["previously_observed_history_review"][1])
    target.mkdir(parents=True)
    shutil.copyfile(spec_path, target / "declaration.json")
    implementation = {"rolling": _sha(__file__), "forward_training_helpers": _sha(Path(__file__).with_name("ashare_forward.py")),
                      "native_trainer": _sha(Path(__file__).parent / "model/trainer.py")}
    records, protocols, used_files = [], [], {}
    started = datetime.now(timezone.utc).isoformat()
    for fold in folds:
        year = fold["execution_year"]
        if progress:
            progress({"phase": "fit", "execution_year": year, "train_start": fold["train_start"], "data_cutoff": fold["data_cutoff"]})
        context = _context(source, calendar, fold)
        used_files.update(context["used_files"])
        fold_output = target / f"M1_{year}"
        if reused:
            previous = reused / f"M1_{year}"
            prior_manifest = json.loads((previous / "manifest.json").read_text())
            for name, digest in prior_manifest["files"].items():
                relative = Path(name)
                if relative.is_absolute() or ".." in relative.parts or _sha(previous / relative) != digest:
                    raise ValueError("Reused annual fit checksum differs from its original native artifact")
            protocol = json.loads((previous / "protocol.json").read_text())
            if (any(protocol[key] != value for key, value in fold.items())
                    or protocol["source_snapshot_manifest_sha256"] != source.manifest_sha256
                    or protocol["declaration_sha256"] != declared_sha
                    or protocol["implementation_sha256"]["rolling"] != reuse_receipt["original_fit_implementation_sha256"]
                    or protocol["used_files_sha256"] != context["used_files"]):
                raise ValueError("Reused fit chronology/source/declaration changed")
            rolling = load_fixed_qlib_model(previous / "model")
            processor = json.loads((previous / "processors.json").read_text())
            if rolling.spec != FixedQlibLGBSpec() or processor["processors"] != rolling.metadata["fitted_inference_processors"]:
                raise ValueError("Reused fit parameters/preprocessing changed")
            maturity_audit = {key: protocol[key] for key in ("training_rows", "first_training_signal", "last_training_signal", "last_label_execution_end")}
            shutil.copytree(previous, fold_output)
        else:
            dataset, handler = _training_dataset(context, fold["train_start"], fold["data_cutoff"])
            learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
            maturity_audit = validate_training_rows(learned, fold, calendar)
            rolling = fit_fixed_qlib_lightgbm(dataset, spec=FixedQlibLGBSpec())
            processor = _processor_payload(handler, rolling.feature_names)
            fold_output.mkdir()
            protocol = {"policy": POLICY, "model": "M1", **fold, **maturity_audit,
            "fitted_at": datetime.now(timezone.utc).isoformat(), "fit_role": "retrospective_reconstructed_history",
            "historical_model_freeze_claim": False, "label": LABEL, "model_spec": asdict(FixedQlibLGBSpec()),
            "train_params": FixedQlibLGBSpec().params(), "normalization_fit_end": fold["train_signal_end"],
            "preprocessing": "RobustZScoreNorm and feature Fillna; learning DropnaLabel and within-training-session CSRankNorm",
            "hyperparameter_search": False, "early_stopping": False,
            "declaration_sha256": declared_sha, "source_snapshot_manifest_sha256": source.manifest_sha256,
                "used_files_sha256": context["used_files"], "implementation_sha256": implementation}
            _write(fold_output / "protocol.json", protocol)
            _write(fold_output / "processors.json", processor)
            rolling.save_native(fold_output / "model", metadata={"model": "M1", "execution_year": year,
                "fitted_inference_processors": processor["processors"], "declaration_sha256": declared_sha,
                "fit_role": "retrospective_reconstructed_history", "train_signal_end": fold["train_signal_end"]})
            # Release the large five-year training table before loading inference.
            del learned, dataset, handler
            gc.collect()
        features, inference_symbols = _raw_features(source, calendar, fold)
        for symbol in inference_symbols:
            for field in REQUIRED_FIELDS:
                name = f"features/{symbol.lower()}/{field}.day.bin"
                used_files[name] = source.manifest["files"][name]["sha256"]
        for model_name, model, processors in (("M0", baseline, baseline.metadata["fitted_inference_processors"]),
                                               ("M1", rolling, processor["processors"])):
            scores = apply_frozen_processors(features, model, processors).rename("score").reset_index()
            scores["datetime"] = pd.to_datetime(scores["datetime"]).dt.strftime("%Y-%m-%d")
            scores["model"], scores["execution_year"] = model_name, year
            records.append(scores[["model", "execution_year", "datetime", "instrument", "score"]])
        if not reused:
            _write(fold_output / "manifest.json", {"schema_version": 1, "execution_year": year,
            "model_sha256": _sha(fold_output / "model/model.txt"), "training_rows": rolling.training_rows,
                "files": {str(file.relative_to(fold_output)): _sha(file) for file in sorted(fold_output.rglob("*")) if file.is_file()}})
        protocols.append({**fold, **maturity_audit, "model_sha256": _sha(fold_output / "model/model.txt"), "fitted_at": protocol["fitted_at"]})
        if progress:
            progress({"phase": "fold_complete", "execution_year": year, "training_rows": rolling.training_rows,
                      "predicted_rows_per_model": len(features)})
        del features, rolling, context
        gc.collect()
    predictions = pd.concat(records, ignore_index=True).sort_values(["model", "execution_year", "datetime", "instrument"])
    if predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
        raise ValueError("Duplicate scores within a model/execution-year fold")
    original = pd.read_csv(origin / "predictions.csv", float_precision="round_trip")
    rebuilt = predictions[predictions.model.eq("M0")].copy()
    rebuilt = rebuilt.sort_values("execution_year").drop_duplicates(["datetime", "instrument"], keep="last")
    overlap = original.merge(rebuilt, on=["datetime", "instrument"], suffixes=("_original", "_rebuilt"), validate="one_to_one")
    max_error = float((overlap.score_original - overlap.score_rebuilt).abs().max()) if len(overlap) else None
    reproduction = {"original_rows": len(original), "overlap_rows": len(overlap), "max_absolute_score_error": max_error,
                    "atol": 1e-11, "rtol": 1e-11, "refit_baseline": False}
    if len(overlap) != len(original) or not np.allclose(overlap.score_original, overlap.score_rebuilt, rtol=1e-11, atol=1e-11):
        _write(target / "blocked_baseline_reproduction.json", reproduction)
        raise ValueError("Saved-processor M0 inference failed to reproduce the frozen baseline scores")
    if _sha(spec_path) != declared_sha or _sha(origin / "manifest.json") != baseline_manifest_sha or _sha(origin / "model/model.txt") != baseline_model_sha:
        raise ValueError("Frozen declaration/baseline changed while fitting rolling research")
    if any(_sha(source.dataset_dir / name) != digest for name, digest in used_files.items()):
        raise ValueError("Research source bytes changed while fitting rolling models")
    predictions.to_csv(target / "predictions.csv", index=False, float_format="%.17g")
    _write(target / "source_files.json", used_files)
    _write(target / "baseline_reproduction.json", reproduction)
    manifest = {"schema_version": 1, "policy": POLICY, "started_at": started, "completed_at": datetime.now(timezone.utc).isoformat(),
        "training_role": "retrospective_reconstructed_history", "historical_model_freeze_claim": False,
        "declaration_sha256": declared_sha, "source_snapshot": str(source.path), "source_manifest_sha256": source.manifest_sha256,
        "baseline_origin": str(origin), "baseline_manifest_sha256": baseline_manifest_sha, "baseline_model_sha256": baseline_model_sha,
        "baseline_reproduction": reproduction, "folds": protocols, "prediction_rows": len(predictions),
        "models": ["M0", "M1"], "hyperparameter_search": False, "early_stopping": False,
        "selection_performed": False, "active_paper_changed": False, "implementation_sha256": implementation, "reused_fits": reuse_receipt,
        "limitations": spec["limitations"],
        "files": {str(file.relative_to(target)): _sha(file) for file in sorted(target.rglob("*")) if file.is_file()}}
    _write(target / "manifest.json", manifest)
    return {"manifest": manifest, "predictions": predictions, "path": target}
