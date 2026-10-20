"""Pure H5/H20 target comparison on a shared,21-session-mature training panel.

The original research modules, models and label globals remain unchanged. Each
annual panel fits feature processors once; two one-target views consume exactly
the same normalized feature bytes and independently ranked target columns.
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

from stopat30m import ashare_union_research as primary
from stopat30m import ashare_stability_research as stability
from stopat30m.ashare_forward import _processor_payload
from stopat30m.ashare_rolling_research import apply_frozen_processors
from stopat30m.data.ashare_scope import load_verified_historical_scope
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot, read_qlib_field
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm


MODELS = ("H5", "H20")
LABELS = {"H5": "Ref($open,-6)/Ref($open,-1)-1", "H20": "Ref($open,-21)/Ref($open,-1)-1"}
TRAINING = {"execution_years": [2025, 2026], "evaluation_end": "2026-10-19",
    "lookback_calendar_years": 5, "common_purge_future_sessions": 21,
    "entry_delay_sessions": 1, "labels": LABELS, "common_endpoint_offsets": [1, 6, 21],
    "membership_filter": "signal_date_only_no_future_membership_survival_condition",
    "shared_feature_processors": True, "common_training_panel": True,
    "fixed_lightgbm_spec": asdict(FixedQlibLGBSpec()), "hyperparameter_search": False, "early_stopping": False}
PACKAGE_KIND = "a8_shared_panel_horizon_predictions"
FEATURE_DIGEST_METHOD = "sha256(index_csv_bytes + canonical_feature_names + each_column_numpy_dtype_and_contiguous_bytes)"


def implementation_sha256():
    return {**primary.implementation_sha256(), "horizon_training": primary._sha(__file__),
        "shared_market_gate": primary._sha(stability.__file__)}


def common_mature_signal_end(calendar, cutoff):
    days = pd.DatetimeIndex(pd.to_datetime(calendar))
    if days.empty or days.has_duplicates or not days.is_monotonic_increasing or pd.Timestamp(cutoff) not in days:
        raise ValueError("Shared maturity needs a unique actual calendar and observed cutoff")
    location = days.get_loc(pd.Timestamp(cutoff))
    if location < 21:
        raise ValueError("Common labels need21 future actual sessions")
    return days[location-21].date().isoformat()


def horizon_fold_plan(calendar, *, evaluation_end="2026-10-19"):
    days = pd.DatetimeIndex(pd.to_datetime(calendar))
    common_mature_signal_end(days, evaluation_end)
    if evaluation_end != TRAINING["evaluation_end"]:
        raise ValueError("Evaluation must keep the predeclared2026-10-19 end")
    result = []
    for year in (2025, 2026):
        prior = days[days < pd.Timestamp(f"{year}-01-01")]
        current = days[(days >= pd.Timestamp(f"{year}-01-01")) & (days <= min(pd.Timestamp(f"{year}-12-31"), pd.Timestamp(evaluation_end)))]
        if prior.empty or current.empty:
            raise ValueError("Each annual arm requires actual preceding and execution sessions")
        cutoff = prior[-1].date().isoformat()
        result.append({"execution_year": year, "train_start": f"{year-5}-01-01", "data_cutoff": cutoff,
            "train_signal_end": common_mature_signal_end(days, cutoff), "signal_start": cutoff,
            "signal_end": current[-1].date().isoformat(), "first_execution_session": current[0].date().isoformat()})
    return result


def common_training_mask(eligibility, index, intervals, *, cutoff, labels):
    """Future endpoint availability/identity is shared; membership is signal-only."""
    if not isinstance(labels, pd.DataFrame) or list(labels.columns) != list(MODELS) or not labels.index.equals(index):
        raise ValueError("Both aligned raw labels must be supplied before shared filtering")
    keep, signal_audit = eligibility.mask(index, intervals)  # Inference-only branch has no old+6 maturity.
    keep = keep.copy()
    calendar = eligibility.calendar
    location = calendar.get_indexer([pd.Timestamp(cutoff)])[0]
    if location < 21:
        raise ValueError("Shared training cutoff needs21 observed future sessions")
    dates = pd.DatetimeIndex(index.get_level_values("datetime"))
    symbols = np.asarray(index.get_level_values("instrument"))
    positions = calendar.get_indexer(dates)
    mature = (positions >= 0) & (positions+21 <= location)
    excluded = {"immature_common_label": int((keep & ~mature).sum())}
    keep &= mature
    endpoint_ok = np.zeros(len(index), dtype=bool)
    for inst in pd.unique(symbols):
        rows = np.flatnonzero((symbols == inst) & mature)
        quotes = eligibility.quote_complete.get(inst)
        if quotes is None:
            continue
        legal = np.ones(len(rows), dtype=bool)
        for offset in (1, 6, 21):
            endpoint = positions[rows]+offset
            legal &= np.asarray(quotes, dtype=bool)[endpoint] & eligibility._identity_mask(inst, calendar[endpoint])
        endpoint_ok[rows] = legal
    excluded["unobserved_or_illegal_common_endpoints"] = int((keep & ~endpoint_ok).sum())
    keep &= endpoint_ok
    finite = np.isfinite(labels.to_numpy(dtype=float)).all(axis=1)
    excluded["missing_either_label"] = int((keep & ~finite).sum())
    keep &= finite
    return keep, {"signal_eligibility": signal_audit, "common_exclusions_in_application_order": excluded,
        "input_rows": len(index), "retained_rows": int(keep.sum()), "common_future_sessions": 21,
        "common_endpoint_offsets": [1, 6, 21], "future_membership_conditioned": False}


def validate_common_training_rows(frame, fold, calendar):
    if frame.empty or not isinstance(frame.index, pd.MultiIndex) or frame.index.has_duplicates:
        raise ValueError("Shared learning requires a nonempty unique dated panel")
    days = pd.DatetimeIndex(pd.to_datetime(calendar))
    dates = pd.DatetimeIndex(frame.index.get_level_values("datetime"))
    positions = days.get_indexer(dates)
    if (not dates.isin(days).all() or dates.min() < pd.Timestamp(fold["train_start"])
            or dates.max() > pd.Timestamp(fold["train_signal_end"])
            or np.any(positions+21 >= len(days))
            or days[positions.max()+21] > pd.Timestamp(fold["data_cutoff"])):
        raise ValueError("A shared21-session training label crosses the fold boundary")
    return {"training_rows": len(frame), "first_training_signal": dates.min().date().isoformat(),
        "last_training_signal": dates.max().date().isoformat(),
        "last_label_execution_end": days[positions.max()+21].date().isoformat(), "common_future_sessions": 21}


def _index_bytes(index):
    if (not isinstance(index, pd.MultiIndex) or set(index.names) != {"datetime", "instrument"}
            or index.has_duplicates):
        raise ValueError("Feature provenance requires a unique dated security index")
    frame = pd.DataFrame({"datetime": pd.DatetimeIndex(index.get_level_values("datetime")).strftime("%Y-%m-%d"),
        "instrument": index.get_level_values("instrument").astype(str)})
    return frame.to_csv(index=False).encode()


def feature_digest(features):
    if len(features.columns) != 158 or features.columns.has_duplicates:
        raise ValueError("A shared official158-feature panel is required")
    digest = hashlib.sha256(_index_bytes(features.index))
    digest.update(json.dumps(list(features.columns), separators=(",", ":")).encode())
    for name in features.columns:
        values = features[name].to_numpy()
        if not np.issubdtype(values.dtype, np.number) or np.isinf(values).any():
            raise ValueError("Feature digest requires finite-or-missing numeric source values")
        digest.update(values.dtype.str.encode()+b"\0")
        digest.update(np.ascontiguousarray(values).tobytes())
    return digest.hexdigest()


class SharedArmDataset:
    """One-target view: the native trainer cannot accidentally consume label0 twice."""

    def __init__(self, learned, arm, expected_feature_sha256):
        if arm not in MODELS or list(learned["label"].columns) != list(MODELS):
            raise ValueError("A declared single-target view of the shared double-label panel is required")
        self.learned, self.arm, self.expected_feature_sha256 = learned, arm, expected_feature_sha256
        self.consumed_feature_sha256 = None
        self.consumed_label_sha256 = None
        self.consume_count = 0

    def prepare(self, segment, col_set=None, data_key=None):
        if segment != "train" or data_key != "learn" or col_set != ["feature", "label"]:
            raise ValueError("Shared arm dataset is only a fixed native training view")
        features = self.learned["feature"]
        digest = feature_digest(features)
        if digest != self.expected_feature_sha256 or not np.isfinite(features.to_numpy()).all():
            raise ValueError("Arm training feature bytes differ from the shared fitted panel")
        self.consumed_feature_sha256, self.consume_count = digest, self.consume_count+1
        label = self.learned["label"][[self.arm]].rename(columns={self.arm: "LABEL0"})
        self.consumed_label_sha256 = hashlib.sha256(np.ascontiguousarray(label.iloc[:, 0].to_numpy()).tobytes()).hexdigest()
        return pd.concat({"feature": features, "label": label}, axis=1)


def prepare_shared_training(frame, fold, calendar):
    """Fit feature processors once; independently CS-rank both raw target columns."""
    if list(frame["label"].columns) != list(MODELS) or not np.isfinite(frame["label"].to_numpy()).all():
        raise ValueError("Common raw panel must have both finite labels before processing")
    validate_common_training_rows(frame, fold, calendar)
    dataset, handler = primary._train_dataset(frame, fold)
    learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    if not learned.index.equals(frame.index):
        raise ValueError("Shared processing changed the common row panel")
    expected = (frame["label"].groupby("datetime").rank(pct=True)-.5)*3.46
    pd.testing.assert_frame_equal(learned["label"], expected, check_dtype=False)
    digest = feature_digest(learned["feature"])
    processors = _processor_payload(handler, list(learned["feature"].columns))
    return learned, processors, digest


def _raw_features(snapshot, calendar, intervals, start, end):
    import qlib
    from qlib.contrib.data.handler import Alpha158
    from qlib.data.dataset.handler import DataHandlerLP
    qlib.init(provider_uri=str(snapshot.dataset_dir), region="cn", kernels=1, expression_cache=None, dataset_cache=None)
    location = int(calendar.searchsorted(pd.Timestamp(start)))
    if location < 80:
        raise ValueError("Official Alpha158 requires80 actual warmup sessions")
    selected = {inst: [(left, right) for left, right in spans if right >= pd.Timestamp(start) and left <= pd.Timestamp(end)]
        for inst, spans in intervals.items()}
    selected = {inst: spans for inst, spans in selected.items() if spans}
    handler = Alpha158(instruments=selected, start_time=calendar[location-80].date().isoformat(), end_time=end,
        infer_processors=[], learn_processors=[], label=(list(LABELS.values()), list(MODELS)))
    frame = handler.fetch(selector=slice(start, end), col_set=["feature", "label"], data_key=DataHandlerLP.DK_R)
    if (frame.empty or frame.index.has_duplicates or len(frame["feature"].columns) != 158
            or list(frame["label"].columns) != list(MODELS)
            or frame.index.get_level_values("datetime").min() < pd.Timestamp(start)
            or frame.index.get_level_values("datetime").max() > pd.Timestamp(end)):
        raise ValueError("Official Alpha158 dual-label schema differs")
    return frame


def _verify_raw_labels(frame, source, calendar):
    """Check the actual single-target columns against their frozen open buffers."""
    positions = calendar.get_indexer(pd.DatetimeIndex(frame.index.get_level_values("datetime")))
    instruments = np.asarray(frame.index.get_level_values("instrument"))
    for inst in pd.unique(instruments):
        locations = np.flatnonzero(instruments == inst)
        values = read_qlib_field(source.dataset_dir/f"features/{inst.lower()}/open.day.bin", calendar)
        for arm, endpoint in (("H5", 6), ("H20", 21)):
            expected = values[positions[locations]+endpoint]/values[positions[locations]+1]-1
            actual = frame["label"][arm].iloc[locations].to_numpy()
            # Provider buffers/official expressions use float32 operations; this
            # is a numerical-expression check, not a relaxed market-price gate.
            if not np.allclose(actual, expected, rtol=8*np.finfo(np.float32).eps, atol=8*np.finfo(np.float32).eps):
                raise ValueError("Raw horizon label differs from its actual frozen open endpoints")


def _load_protocol(path, source, scope_directory):
    protocol = json.loads(Path(path).read_bytes())
    if (protocol.get("kind") != "a8_shared_panel_horizon_protocol" or protocol.get("schema_version") != 1
            or protocol.get("models") != list(MODELS) or protocol.get("training") != TRAINING
            or protocol.get("source_snapshot_manifest_sha256") != source.manifest_sha256
            or protocol.get("historical_scope_manifest_sha256") != primary._sha(Path(scope_directory)/"manifest.json")
            or protocol.get("implementation_sha256") != implementation_sha256()):
        raise ValueError("A sealed common21-session, shared-panel H5/H20 protocol is required")
    from stopat30m.ashare_stability_validation import _assert_proofs
    _assert_proofs(protocol["bound_inputs"])
    return protocol


def _context(protocol_path):
    preliminary = json.loads(Path(protocol_path).read_bytes())
    bound = preliminary["bound_inputs"]
    from stopat30m.ashare_stability_validation import _assert_proofs
    _assert_proofs(bound)
    bundle = json.loads(Path(bound["source_bundle"]["path"]).read_bytes())
    source = load_qlib_snapshot(bundle["snapshot_path"], verify=True)
    scope_directory = Path(bundle["scope_path"])
    protocol = _load_protocol(protocol_path, source, scope_directory)
    if (primary._sha(source.path/"manifest.json") != bound["source_snapshot_manifest"]["sha256"]
            or Path(bound["source_snapshot_manifest"]["path"]).resolve() != source.path/"manifest.json"
            or primary._sha(scope_directory/"manifest.json") != bound["historical_scope_manifest"]["sha256"]
            or Path(bound["historical_scope_manifest"]["path"]).resolve() != (scope_directory/"manifest.json").resolve()):
        raise ValueError("Consumed source/scope differs from bound protocol paths")
    calendar = pd.DatetimeIndex(pd.to_datetime((source.dataset_dir/"calendars/day.txt").read_text().splitlines()))
    folds = horizon_fold_plan(calendar)
    audit = json.loads(Path(bound["market_audit"]["path"]).read_bytes())
    basic = bound["stock_basic"]["path"]
    stability.validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=primary._sha(scope_directory/"manifest.json"), stock_basic_sha=primary._sha(basic),
        coverage_start=folds[0]["first_execution_session"], coverage_end=TRAINING["evaluation_end"])
    primary.assert_market_originals_unchanged(audit, basic)
    return protocol, source, scope_directory, calendar, folds, audit, basic


def build_horizon_predictions(protocol_path, output_dir, *, progress=None):
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Preserve every prior shared-panel model generation")
    protocol, source, scope_directory, calendar, folds, audit, basic = _context(protocol_path)
    implementation = implementation_sha256()
    inputs = {**protocol["bound_inputs"], "declaration": {"path": str(Path(protocol_path).resolve()), "sha256": primary._sha(protocol_path)}}
    scope = load_verified_historical_scope(scope_directory, start=folds[0]["train_start"], end=TRAINING["evaluation_end"], require_official=True)
    with scope.verified_batch():
        membership_audit = scope.audit_sessions([day.date().isoformat() for day in calendar if scope.start <= day.date().isoformat() <= scope.end])
        intervals = primary.union_intervals(scope, scope.start, scope.end)
    if membership_audit["status"] != "passed":
        raise ValueError("Official historical membership must cover every training/inference session")
    eligibility, source_files = primary.load_eligibility(source, calendar, intervals, basic)
    source_files["calendars/day.txt"] = source.manifest["files"]["calendars/day.txt"]["sha256"]
    target.mkdir(parents=True)
    shutil.copyfile(protocol_path, target/"declaration.json")
    shutil.copyfile(__file__, target/"horizon_training_implementation.py")
    primary._write(target/"membership_audit.json", membership_audit)
    started, all_scores, fold_records = datetime.now(timezone.utc).isoformat(), [], []
    try:
        for fold in folds:
            year = fold["execution_year"]
            if progress:
                progress({"phase": "load_common_training_features", **fold})
            # Expression extension reaches at most the cutoff, rather than
            # loading immature tail labels past the annual fit boundary.
            raw = _raw_features(source, calendar, intervals, fold["train_start"], fold["train_signal_end"])
            keep, filtering = common_training_mask(eligibility, raw.index, intervals, cutoff=fold["data_cutoff"], labels=raw["label"])
            filtered = raw.loc[keep].copy(); del raw
            if filtered.empty:
                raise ValueError("No shared legal mature training panel remains")
            _verify_raw_labels(filtered, source, calendar)
            raw_feature_sha = feature_digest(filtered["feature"])
            learned, processors, processed_sha = prepare_shared_training(filtered, fold, calendar)
            maturity = validate_common_training_rows(learned, fold, calendar)
            if not primary.dated_membership_mask(learned.index, intervals).all():
                raise ValueError("An out-of-signal-membership row reached the shared training panel")
            shared = target/f"shared_{year}"; shared.mkdir()
            index_bytes = _index_bytes(learned.index)
            (shared/"training_index.csv").write_bytes(index_bytes)
            primary._write(shared/"processors.json", processors)
            panel = {**fold, **maturity, "training_filtering": filtering,
                "training_index_sha256": hashlib.sha256(index_bytes).hexdigest(),
                "raw_features_sha256": raw_feature_sha, "processed_features_sha256": processed_sha,
                "feature_digest_method": FEATURE_DIGEST_METHOD,
                "feature_dtypes": [str(dtype) for dtype in learned["feature"].dtypes],
                "processors_sha256": primary._sha(shared/"processors.json"),
                "ranked_label_sha256": {arm: hashlib.sha256(np.ascontiguousarray(learned["label"][arm].to_numpy()).tobytes()).hexdigest() for arm in MODELS},
                "feature_processors_fit_count": 1, "independent_label_rank_normalization": True}
            primary._write(shared/"panel.json", panel)
            del filtered; gc.collect()
            models, records = {}, {}
            for arm in MODELS:
                view = SharedArmDataset(learned, arm, processed_sha)
                if progress:
                    progress({"phase": "fit", "model": arm, "execution_year": year, **maturity})
                model = fit_fixed_qlib_lightgbm(view, spec=FixedQlibLGBSpec())
                if (view.consume_count < 1 or view.consumed_feature_sha256 != processed_sha
                        or view.consumed_label_sha256 != panel["ranked_label_sha256"][arm]
                        or feature_digest(learned["feature"]) != processed_sha):
                    raise ValueError("Native trainer did not consume unchanged shared feature bytes")
                folder = target/f"{arm}_{year}"; folder.mkdir()
                shutil.copyfile(shared/"processors.json", folder/"processors.json")
                fitted_at = datetime.now(timezone.utc).isoformat()
                model.save_native(folder/"model", metadata={"model": arm, "execution_year": year,
                    "label": LABELS[arm], "label_holding_sessions": 5 if arm == "H5" else 20,
                    "fitted_at": fitted_at, "common_purge_future_sessions": 21,
                    "fitted_inference_processors": processors["processors"], "declaration_sha256": inputs["declaration"]["sha256"],
                    "fit_role": "new_retrospective_common_panel_label_control_not_reused_old_model",
                    "train_signal_end": fold["train_signal_end"], "training_index_sha256": panel["training_index_sha256"],
                    "shared_training_features_sha256": processed_sha, "feature_processors_sha256": panel["processors_sha256"]})
                records[arm] = {**fold, **maturity, "model": arm, "label": LABELS[arm], "fitted_at": fitted_at,
                    "common_training_index_sha256": panel["training_index_sha256"],
                    "consumed_training_features_sha256": view.consumed_feature_sha256,
                    "consumed_ranked_label_sha256": view.consumed_label_sha256,
                    "shared_processors_sha256": primary._sha(folder/"processors.json"),
                    "model_sha256": primary._sha(folder/"model/model.txt"), "historical_model_freeze_claim": False}
                models[arm] = model
            del learned; gc.collect()
            if progress:
                progress({"phase": "load_shared_inference_features", **fold})
            raw = _raw_features(source, calendar, intervals, fold["signal_start"], fold["signal_end"])
            keep, inference_filter = eligibility.mask(raw.index, intervals)  # Never future-label-filter inference.
            features = raw.loc[keep, "feature"].copy(); del raw
            inference_sha = feature_digest(features)
            for arm in MODELS:
                model, folder = models[arm], target/f"{arm}_{year}"
                scores = apply_frozen_processors(features, model, processors["processors"]).rename("score").reset_index()
                if feature_digest(features) != inference_sha:
                    raise ValueError("One model changed the shared inference feature panel")
                scores["datetime"] = pd.to_datetime(scores["datetime"]).dt.strftime("%Y-%m-%d")
                scores["model"], scores["execution_year"] = arm, year
                scores = scores[["model", "execution_year", "datetime", "instrument", "score"]]
                scores = scores.sort_values(["datetime", "instrument"]).reset_index(drop=True)
                scores.to_csv(folder/"predictions.csv", index=False, float_format="%.17g")
                all_scores.append(scores)
                records[arm].update(inference_filtering=inference_filter, inference_feature_sha256=inference_sha,
                    prediction_rows=len(scores), inference_future_label_filter_applied=False)
                primary._write(folder/"protocol.json", records[arm])
                primary._write(folder/"manifest.json", {"files": {str(path.relative_to(folder)): primary._sha(path)
                    for path in sorted(folder.rglob("*")) if path.is_file()}})
            fold_records.append({**fold, "shared_panel": panel, "arms": records})
            if progress:
                progress({"phase": "fold_complete", "execution_year": year,
                    "training_rows_per_arm": maturity["training_rows"], "prediction_rows_per_arm": len(scores)})
            del features, models; gc.collect()
        predictions = pd.concat(all_scores, ignore_index=True).sort_values(["model", "execution_year", "datetime", "instrument"])
        if predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
            raise ValueError("Duplicate model/year/signal/security prediction")
        from stopat30m.ashare_stability_validation import _assert_proofs
        _assert_proofs(inputs); scope.assert_unchanged()
        primary.assert_market_originals_unchanged(audit, basic)
        if implementation_sha256() != implementation or any(primary._sha(source.dataset_dir/name) != digest for name, digest in source_files.items()):
            raise ValueError("Source feature or training implementation bytes changed")
        predictions.to_csv(target/"predictions.csv", index=False, float_format="%.17g")
        primary._write(target/"source_files.json", source_files)
        manifest = {"schema_version": 1, "kind": PACKAGE_KIND, "models": list(MODELS), "training": TRAINING,
            "started_at": started, "completed_at": datetime.now(timezone.utc).isoformat(), "inputs": inputs,
            "source_snapshot": str(source.path), "source_manifest_sha256": source.manifest_sha256,
            "historical_scope_manifest_sha256": primary._sha(scope_directory/"manifest.json"),
            "scope_sources": scope.source_receipts(), "folds": fold_records, "prediction_rows": len(predictions),
            "native_fit_count": 4, "feature_processor_fit_count": 2, "hyperparameter_search": False,
            "early_stopping": False, "historical_model_freeze_claim": False, "new_future_out_of_sample_claim": False,
            "active_paper_changed": False, "selection_performed": False, "implementation_sha256": implementation,
            "files": {str(path.relative_to(target)): primary._sha(path) for path in sorted(target.rglob("*")) if path.is_file()}}
        primary._write(target/"manifest.json", manifest)
        return {"manifest": manifest, "predictions": predictions, "path": target}
    except Exception as exc:
        primary._write(target/"failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "completed_annual_panels": len(fold_records), "no_completed_model_package_claim": True})
        raise


def verify_horizon_predictions(directory, *, snapshot_path, scope_directory, protocol_path, market_audit_path):
    """Verify the independent package; never present these models as an A5 reuse."""
    root = Path(directory).resolve()
    manifest = json.loads((root/"manifest.json").read_bytes())
    if (manifest.get("kind") != PACKAGE_KIND or manifest.get("models") != list(MODELS)
            or manifest.get("training") != TRAINING or manifest.get("native_fit_count") != 4
            or manifest.get("feature_processor_fit_count") != 2 or manifest.get("implementation_sha256") != implementation_sha256()):
        raise ValueError("A complete unchanged four-fit shared-panel model package is required")
    actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file() and path != root/"manifest.json"}
    if actual != set(manifest["files"]):
        raise ValueError("Model package artifact inventory changed")
    from stopat30m.ashare_stability_validation import _assert_proofs, _proof
    bound = {"horizon_model_manifest": _proof(root/"manifest.json")}
    for name, digest in manifest["files"].items():
        path = (root/name).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Unsafe model package artifact path")
        bound["horizon_model:"+name] = {"path": str(path), "sha256": digest}
    _assert_proofs(bound); _assert_proofs(manifest["inputs"]); _assert_proofs(manifest["scope_sources"])
    protocol, source, scope, calendar, folds, audit, basic = _context(protocol_path)
    if (Path(snapshot_path).resolve() != source.path or Path(scope_directory).resolve() != scope.resolve()
            or Path(market_audit_path).resolve() != Path(protocol["bound_inputs"]["market_audit"]["path"]).resolve()
            or manifest["inputs"]["declaration"] != _proof(protocol_path)
            or (root/"declaration.json").read_bytes() != Path(protocol_path).read_bytes()
            or manifest["source_manifest_sha256"] != source.manifest_sha256
            or manifest["historical_scope_manifest_sha256"] != primary._sha(scope/"manifest.json")):
        raise ValueError("Model package differs from the exact declared source generation")
    predictions = pd.read_csv(root/"predictions.csv", dtype={"model": str, "datetime": str, "instrument": str})
    if (list(predictions.columns) != ["model", "execution_year", "datetime", "instrument", "score"]
            or set(predictions.model) != set(MODELS) or len(predictions) != manifest["prediction_rows"]
            or predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any()
            or not np.isfinite(predictions.score.to_numpy()).all() or len(manifest["folds"]) != 2):
        raise ValueError("Invalid fixed two-arm prediction schema or coverage")
    for expected_fold, record in zip(folds, manifest["folds"]):
        if any(record.get(key) != value for key, value in expected_fold.items()) or set(record["arms"]) != set(MODELS):
            raise ValueError("Annual common maturity/arm coverage differs")
        year = record["execution_year"]
        shared = root/f"shared_{year}"
        panel = json.loads((shared/"panel.json").read_bytes())
        if (panel != record["shared_panel"] or panel["feature_processors_fit_count"] != 1
                or primary._sha(shared/"training_index.csv") != panel["training_index_sha256"]):
            raise ValueError("Shared panel or actual training index proof changed")
        arm_index = None
        for arm in MODELS:
            folder = root/f"{arm}_{year}"; arm_record = json.loads((folder/"protocol.json").read_bytes())
            native = json.loads((folder/"model/manifest.json").read_bytes())
            metadata = native["metadata"]
            if (arm_record != record["arms"][arm] or arm_record["label"] != LABELS[arm]
                    or arm_record["common_training_index_sha256"] != panel["training_index_sha256"]
                    or arm_record["consumed_training_features_sha256"] != panel["processed_features_sha256"]
                    or arm_record["consumed_ranked_label_sha256"] != panel["ranked_label_sha256"][arm]
                    or arm_record["shared_processors_sha256"] != panel["processors_sha256"]
                    or (folder/"processors.json").read_bytes() != (shared/"processors.json").read_bytes()
                    or metadata["model"] != arm or metadata["label"] != LABELS[arm]
                    or metadata["training_index_sha256"] != panel["training_index_sha256"]
                    or metadata["shared_training_features_sha256"] != panel["processed_features_sha256"]
                    or metadata["fitted_at"] != arm_record["fitted_at"] or metadata["common_purge_future_sessions"] != 21
                    or native["training_rows"] != panel["training_rows"]
                    or native["model_spec"] != asdict(FixedQlibLGBSpec())
                    or native["model_sha256"] != primary._sha(folder/"model/model.txt")):
                raise ValueError("An arm differs from the shared feature/processor/label contract")
            rows = pd.read_csv(folder/"predictions.csv", dtype={"model": str, "datetime": str, "instrument": str})
            global_rows = predictions.loc[(predictions.model == arm) & (predictions.execution_year == year)].reset_index(drop=True)
            if not rows.equals(global_rows):
                raise ValueError("Global predictions differ from the original annual arm rows")
            index = list(zip(rows.datetime, rows.instrument))
            if arm_index is not None and arm_index != index:
                raise ValueError("Arm inference panels differ or future-label availability was applied")
            arm_index = index
    source_files = json.loads((root/"source_files.json").read_bytes())
    for name, digest in source_files.items():
        if primary._sha(source.dataset_dir/name) != digest:
            raise ValueError("Actual model feature-source bytes changed")
    _assert_proofs(bound); _assert_proofs(manifest["inputs"])
    return {"kind": "a8_shared_panel_model_verification", "status": "passed", "manifest": manifest,
        "model_manifest": _proof(root/"manifest.json"), "bound_inputs": {**bound, **manifest["inputs"]},
        "native_fit_count": 4, "training_verification_performed": True, "training_reperformed": False}


def verify_horizon_model_package(directory, protocol_path):
    preliminary = json.loads(Path(protocol_path).read_bytes())
    bound = preliminary["bound_inputs"]
    bundle = json.loads(Path(bound["source_bundle"]["path"]).read_bytes())
    return verify_horizon_predictions(directory, snapshot_path=bundle["snapshot_path"],
        scope_directory=bundle["scope_path"], protocol_path=protocol_path,
        market_audit_path=bound["market_audit"]["path"])["manifest"]
