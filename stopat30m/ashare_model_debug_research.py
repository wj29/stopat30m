"""Bounded, staged model debugging with unchanged A5 data and paper account.

Three predeclared candidates are fitted on the 2023 selection fold. Only its
recorded winner may reach 2024 confirmation and the already-viewed 2025/2026
retrospective folds. This module never chooses a candidate from model metrics,
changes an existing package, or promotes a model to the daily paper account.
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
from stopat30m.ashare_horizon_research import feature_digest, _index_bytes
from stopat30m.ashare_rolling_research import apply_frozen_processors, validate_training_rows
from stopat30m.data.ashare_scope import load_verified_historical_scope
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot, read_qlib_field
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm, load_fixed_qlib_model


KIND = "a14_model_debug_training_protocol"
PACKAGE_KIND = "a14_staged_model_debug_predictions"
CANDIDATES = {
    "capacity300": {"lookback_calendar_years": 5,
        "spec": asdict(FixedQlibLGBSpec(num_boost_round=300))},
    "regularized300": {"lookback_calendar_years": 5,
        "spec": asdict(FixedQlibLGBSpec(num_boost_round=300, num_leaves=15,
            max_depth=4, min_data_in_leaf=500, lambda_l2=10.0))},
    "recent3_capacity300": {"lookback_calendar_years": 3,
        "spec": asdict(FixedQlibLGBSpec(num_boost_round=300))},
}
PHASE_YEARS = {"selection": [2023], "confirmation": [2024], "retrospective": [2025, 2026]}
LABEL = primary.LABEL
DESIGN_CANONICAL_SHA256 = "bdd3f763e19e38fca6328e4a515c236e09510a1c317e0af1d18cfe9528dbeba5"


def _read(path):
    return json.loads(Path(path).read_bytes())


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": primary._sha(path)}


def _assert_proof(proof):
    if (not isinstance(proof, dict) or not isinstance(proof.get("path"), str)
            or not Path(proof["path"]).is_absolute() or not Path(proof["path"]).is_file()
            or primary._sha(proof["path"]) != proof.get("sha256")):
        raise ValueError("A bound model-debug input changed or lacks an absolute proof")


def _assert_proofs(proofs):
    if not isinstance(proofs, dict) or not proofs:
        raise ValueError("Nonempty bound input proofs are required")
    for proof in proofs.values():
        _assert_proof(proof)


def implementation_sha256():
    return {**primary.implementation_sha256(), "stability_training": primary._sha(stability.__file__),
        "model_debug_training": primary._sha(__file__),
        "feature_digest": primary._sha(feature_digest.__code__.co_filename)}


def _verify_files(root, files):
    if not isinstance(files, dict) or not files:
        raise ValueError("Original model package requires its complete file inventory")
    for name, digest in files.items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or not (root/relative).is_file():
            raise ValueError("Original model package contains an invalid relative file")
        if primary._sha(root/relative) != digest:
            raise ValueError("Original model package bytes changed")


def _scope_directory(manifest, digest):
    proofs = manifest.get("scope_sources", {})
    _assert_proofs(proofs)
    found = {Path(proof["path"]).resolve().parent for proof in proofs.values()
        if Path(proof["path"]).name == "manifest.json" and proof.get("sha256") == digest}
    if len(found) != 1:
        raise ValueError("Exactly one unchanged official historical scope is required")
    return found.pop()


def _load_declaration(path):
    declaration = _read(path)
    if (declaration.get("kind") != KIND or declaration.get("schema_version") != 1
            or any(declaration.get("implementation_sha256", {}).get(key) != value
                for key, value in implementation_sha256().items())):
        raise ValueError("A current, source-bound model-debug training declaration is required")
    _assert_proof(declaration.get("bound_design"))
    design = _read(declaration["bound_design"]["path"])
    canonical = json.dumps(design, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    if hashlib.sha256(canonical).hexdigest() != DESIGN_CANONICAL_SHA256:
        raise ValueError("The complete pre-result model-debug design differs from its frozen version")
    # Candidate mechanics are executable constants. The bound human design may
    # additionally specify execution/selection gates owned by the experiment.
    declared = declaration.get("candidates", CANDIDATES)
    if declared != CANDIDATES:
        raise ValueError("Only the three frozen 300-round candidate mechanics are supported")
    if set(declaration.get("source_families", {})) != {"primary", "stability"}:
        raise ValueError("Primary and stability source families must be independently bound")
    for family in declaration["source_families"].values():
        if set(family) != {"model_manifest", "protocol", "market_audit", "stock_basic"}:
            raise ValueError("Each family must bind its original model, protocol, audit and identity input")
        _assert_proofs(family)
    return declaration, design


def phase_plan(declaration, *, phase, selection_path=None):
    """Validate the immutable 3/1/2 fit budget before loading any feature rows."""
    if phase not in PHASE_YEARS:
        raise ValueError("Unknown model-debug phase")
    if phase == "selection":
        if selection_path is not None:
            raise ValueError("Selection cannot consume a later candidate decision")
        return {"phase": phase, "years": PHASE_YEARS[phase], "candidates": list(CANDIDATES),
            "native_fit_count": 3, "selection_decision": None}
    if selection_path is None:
        raise ValueError("Confirmation and retrospective training require the same prior selection decision")
    decision = _read(selection_path)
    if (decision.get("kind") != "a14_model_debug_selection_decision"
            or decision.get("selected_candidate") not in CANDIDATES):
        raise ValueError("A fixed 2023 candidate decision is required")
    required = {key: decision.get(key) for key in ("selection_report", "selection_model_manifest")}
    _assert_proofs(required)
    selected_manifest = _read(required["selection_model_manifest"]["path"])
    if (selected_manifest.get("kind") != PACKAGE_KIND or selected_manifest.get("phase") != "selection"
            or selected_manifest.get("native_fit_count") != 3
            or selected_manifest.get("execution_years") != [2023]
            or selected_manifest.get("models") != list(CANDIDATES)
            or selected_manifest.get("inputs", {}).get("declaration") != declaration["_self_proof"]):
        raise ValueError("Selection decision must bind this declaration's complete 2023-only fit package")
    _verify_files(Path(required["selection_model_manifest"]["path"]).parent, selected_manifest["files"])
    report = _read(required["selection_report"]["path"])
    report_proof = report.get("model_manifest", report.get("inputs", {}).get("model_manifest"))
    if report_proof != required["selection_model_manifest"]:
        raise ValueError("Selection report must bind the exact 2023 model package")
    if report.get("phase") != "selection":
        raise ValueError("A later report cannot select the 2023 winner")
    selected = report.get("selection", {}).get("selected_candidate", report.get("selected_candidate"))
    if selected != decision["selected_candidate"]:
        raise ValueError("Decision candidate differs from the bound 2023 selection report")
    for label, value in (("decision", decision.get("declared_at")),
                         ("selection package", selected_manifest.get("completed_at"))):
        stamp = pd.Timestamp(value)
        if pd.isna(stamp) or stamp.tzinfo is None:
            raise ValueError(f"The {label} requires an explicit actual timestamp")
    if pd.Timestamp(decision["declared_at"]) < pd.Timestamp(selected_manifest["completed_at"]):
        raise ValueError("A selection decision cannot precede its actual training package")
    return {"phase": phase, "years": PHASE_YEARS[phase], "candidates": [decision["selected_candidate"]],
        "native_fit_count": len(PHASE_YEARS[phase]), "selection_decision": _proof(selection_path),
        "selection_inputs": required}


def _source_context(family, *, family_key):
    _assert_proofs(family)
    package = Path(family["model_manifest"]["path"]).parent
    manifest, protocol, audit = [_read(family[key]["path"]) for key in
        ("model_manifest", "protocol", "market_audit")]
    expected_kind = "a5_union_fixed_predictions" if family_key == "primary" else "a5_union_stability_fixed_predictions"
    if manifest.get("kind") != expected_kind:
        raise ValueError("Model-debug source families cannot be interchanged")
    _verify_files(package, manifest.get("files"))
    if (manifest.get("inputs", {}).get("declaration") != family["protocol"]
            or manifest.get("inputs", {}).get("market_audit") != family["market_audit"]
            or manifest.get("inputs", {}).get("stock_basic") != family["stock_basic"]
            or primary._sha(package/"declaration.json") != family["protocol"]["sha256"]):
        raise ValueError("Old source package, protocol, audit and identities do not agree")
    _assert_proofs(manifest["inputs"])
    source = load_qlib_snapshot(manifest["source_snapshot"], verify=True)
    if (source.manifest_sha256 != manifest.get("source_manifest_sha256")
            or source.manifest_sha256 != protocol.get("source_snapshot_manifest_sha256")):
        raise ValueError("The original source snapshot cannot be replaced by a newer family")
    scope_directory = _scope_directory(manifest, protocol["historical_scope_manifest_sha256"])
    loader = primary if family_key == "primary" else stability
    loader._load_protocol(family["protocol"]["path"], source, scope_directory)
    calendar = pd.DatetimeIndex(pd.to_datetime((source.dataset_dir/"calendars/day.txt").read_text().splitlines()))
    folds = loader.union_fold_plan(calendar, protocol["training"]["execution_years"],
        evaluation_end=protocol["training"]["evaluation_end"])
    loader.validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=protocol["historical_scope_manifest_sha256"], stock_basic_sha=family["stock_basic"]["sha256"],
        coverage_start=folds[0]["first_execution_session"], coverage_end=protocol["training"]["evaluation_end"])
    primary.assert_market_originals_unchanged(audit, family["stock_basic"]["path"])
    if manifest.get("implementation_sha256") != loader.implementation_sha256():
        raise ValueError("Original training helpers changed; do not silently rebind the A5 package")
    files = _read(package/"source_files.json")
    for name, digest in files.items():
        if (source.manifest["files"].get(name, {}).get("sha256") != digest
                or primary._sha(source.dataset_dir/name) != digest):
            raise ValueError("Original consumed source feature bytes changed")
    scope = load_verified_historical_scope(scope_directory, start=folds[0]["train_start"],
        end=protocol["training"]["evaluation_end"], require_official=True)
    with scope.verified_batch():
        audit_members = scope.audit_sessions([day.date().isoformat() for day in calendar
            if scope.start <= day.date().isoformat() <= scope.end])
        intervals = primary.union_intervals(scope, scope.start, scope.end)
    if audit_members["status"] != "passed":
        raise ValueError("Official dated membership must cover each actual model session")
    eligibility, used = primary.load_eligibility(source, calendar, intervals, family["stock_basic"]["path"])
    used["calendars/day.txt"] = source.manifest["files"]["calendars/day.txt"]["sha256"]
    return {"source": source, "calendar": calendar, "scope": scope, "intervals": intervals,
        "eligibility": eligibility, "source_files": used, "membership_audit": audit_members,
        "folds": {fold["execution_year"]: fold for fold in folds}, "audit": audit,
        "stock_basic": family["stock_basic"]["path"], "package": package, "family": family,
        "scope_manifest_sha256": protocol["historical_scope_manifest_sha256"]}


def candidate_fold(fold, candidate):
    if candidate not in CANDIDATES:
        raise ValueError("Unknown frozen model candidate")
    return {**fold, "train_start": f"{fold['execution_year']-CANDIDATES[candidate]['lookback_calendar_years']}-01-01"}


def _label_digest(label):
    values = np.ascontiguousarray(label.to_numpy())
    return hashlib.sha256(values.dtype.str.encode()+b"\0"+values.tobytes()).hexdigest()


class ConsumedPanel:
    """One-target native training view which proves actual consumed bytes."""
    def __init__(self, learned, expected_features, expected_labels):
        self.learned = learned
        self.expected_features, self.expected_labels = expected_features, expected_labels
        self.consume_count = 0
        self.consumed_features = self.consumed_labels = None

    def prepare(self, segment, col_set=None, data_key=None):
        if segment != "train" or col_set != ["feature", "label"] or data_key != "learn":
            raise ValueError("Only the frozen single training panel can reach the native trainer")
        feature_sha = feature_digest(self.learned["feature"])
        label_sha = _label_digest(self.learned["label"].iloc[:, 0])
        if (feature_sha != self.expected_features or label_sha != self.expected_labels
                or len(self.learned["label"].columns) != 1
                or not np.isfinite(self.learned.to_numpy()).all()):
            raise ValueError("The native training feature or target bytes changed")
        self.consume_count += 1
        self.consumed_features, self.consumed_labels = feature_sha, label_sha
        return self.learned


def prepare_candidate_panel(frame, fold, calendar):
    """Fit feature statistics on only this candidate's mature training rows."""
    validate_training_rows(frame, fold, calendar)
    dataset, handler = primary._train_dataset(frame, fold)
    learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    if not learned.index.equals(frame.index):
        raise ValueError("Training processors changed the declared eligible index")
    expected = (frame["label"].groupby("datetime").rank(pct=True)-.5)*3.46
    pd.testing.assert_frame_equal(learned["label"], expected, check_dtype=False)
    processors = primary._processor_payload(handler, list(learned["feature"].columns))
    return learned, processors


def _verify_raw_labels(frame, source, calendar):
    positions = calendar.get_indexer(pd.DatetimeIndex(frame.index.get_level_values("datetime")))
    symbols = np.asarray(frame.index.get_level_values("instrument"))
    for symbol in pd.unique(symbols):
        rows = np.flatnonzero(symbols == symbol)
        values = read_qlib_field(source.dataset_dir/f"features/{symbol.lower()}/open.day.bin", calendar)
        expected = values[positions[rows]+6]/values[positions[rows]+1]-1
        if not np.allclose(frame["label"].iloc[rows, 0].to_numpy(), expected,
                rtol=8*np.finfo(np.float32).eps, atol=8*np.finfo(np.float32).eps):
            raise ValueError("H5 fitting labels differ from the bound next-open endpoints")


def score_diagnostics(scores, labels, *, role):
    """Descriptive daily rank diagnostics; no overlapping-label significance claim."""
    if not scores.index.equals(labels.index) or scores.index.has_duplicates:
        raise ValueError("Diagnostics require exact score/target index alignment")
    paired = pd.DataFrame({"score": scores, "label": labels})
    valid = np.isfinite(paired.to_numpy()).all(axis=1)
    paired = paired.loc[valid]
    rows = []
    for day, group in paired.groupby(level="datetime", sort=True):
        rank_ic = group.score.rank(method="average").corr(group.label.rank(method="average")) if len(group) >= 20 else np.nan
        rows.append({"date": pd.Timestamp(day).date().isoformat(), "observations": len(group),
            "rank_ic": float(rank_ic) if np.isfinite(rank_ic) else None,
            "score_std": float(group.score.std(ddof=0)), "label_std": float(group.label.std(ddof=0))})
    daily = pd.DataFrame(rows, columns=["date", "observations", "rank_ic", "score_std", "label_std"])
    known = pd.to_numeric(daily.rank_ic).dropna()
    monthly = []
    if not daily.empty:
        for month, group in daily.groupby(daily.date.str[:7]):
            values = pd.to_numeric(group.rank_ic).dropna()
            monthly.append({"month": month, "known_days": len(values),
                "mean_rank_ic": float(values.mean()) if len(values) else None})
    return {"role": role, "row_count": len(paired), "excluded_nonfinite_rows": int((~valid).sum()),
        "session_count": len(daily), "known_rank_ic_days": len(known),
        "mean_daily_rank_ic": float(known.mean()) if len(known) else None,
        "positive_rank_ic_day_fraction": float((known > 0).mean()) if len(known) else None,
        "score_std": float(paired.score.std(ddof=0)) if len(paired) else None,
        "daily": daily, "monthly": monthly, "independent_observations_claim": False,
        "in_sample_optimism": role == "training_in_sample"}


def _save_diagnostics(folder, name, diagnostics):
    diagnostics = dict(diagnostics)
    diagnostics.pop("daily").to_csv(folder/f"{name}_daily.csv", index=False, float_format="%.17g")
    primary._write(folder/f"{name}.json", diagnostics)
    return diagnostics


def _copy_baseline(context, year, target):
    origin = context["package"]/f"LGB_{year}"
    destination = target/"baseline_reuse"/f"LGB_{year}"
    destination.mkdir(parents=True)
    for name in ("predictions.csv", "processors.json", "protocol.json"):
        shutil.copyfile(origin/name, destination/name)
    model = load_fixed_qlib_model(origin/"model")
    fold = context["folds"][year]
    if (model.spec != FixedQlibLGBSpec() or model.metadata.get("execution_year") != year
            or model.metadata.get("train_signal_end") != fold["train_signal_end"]):
        raise ValueError("Baseline reuse requires the exact original 100-round annual model")
    return {"execution_year": year, "native_refit": False, "predictions_byte_identical": True,
        "original_predictions": _proof(origin/"predictions.csv"),
        "copied_predictions": _proof(destination/"predictions.csv"),
        "original_native_model": _proof(origin/"model/model.txt"),
        "original_native_manifest": _proof(origin/"model/manifest.json"),
        "original_processors": _proof(origin/"processors.json")}


def build_debug_predictions(declaration_path, output_dir, *, phase, selection_path=None, progress=None):
    """Fit exactly the fixed phase budget; preserve every prior and partial output."""
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Preserve every prior model-debug training output")
    declaration, _ = _load_declaration(declaration_path)
    declaration["_self_proof"] = _proof(declaration_path)
    plan = phase_plan(declaration, phase=phase, selection_path=selection_path)
    active = Path(declaration.get("active_paper_directory",
        Path(__file__).parents[1]/"output/research/ashare/forward/low_turnover_20261020_v1")).resolve()
    if target == active or target.is_relative_to(active) or active.is_relative_to(target):
        raise ValueError("Model debugging must be isolated from the current 20-stock paper account")
    inputs = {"declaration": declaration["_self_proof"], "design": declaration["bound_design"]}
    if plan["selection_decision"]:
        inputs["selection_decision"] = plan["selection_decision"]
        inputs.update(plan["selection_inputs"])
    implementation = implementation_sha256()
    family_keys = {"primary" if year <= 2024 else "stability" for year in plan["years"]}
    contexts = {name: _source_context(declaration["source_families"][name], family_key=name) for name in sorted(family_keys)}
    target.mkdir(parents=True)
    shutil.copyfile(declaration_path, target/"declaration.json")
    shutil.copyfile(__file__, target/"model_debug_training_implementation.py")
    primary._write(target/"phase_plan.json", plan)
    for name, context in contexts.items():
        primary._write(target/f"{name}_membership_audit.json", context["membership_audit"])
        primary._write(target/f"{name}_source_files.json", context["source_files"])
        inputs.update({f"{name}_{key}": proof for key, proof in context["family"].items()})
    records, baseline, all_scores = [], [], []
    started = datetime.now(timezone.utc).isoformat()
    try:
        for year in plan["years"]:
            context = contexts["primary" if year <= 2024 else "stability"]
            source, calendar = context["source"], context["calendar"]
            fold = context["folds"][year]
            baseline.append(_copy_baseline(context, year, target))
            if progress:
                progress({"phase": "load_training_features", "execution_year": year, **fold})
            # End at the mature signal rather than reading the immature tail's
            # labels beyond the annual fit cutoff. The retained index is A5's.
            raw = primary._raw_alpha158(source, calendar, context["intervals"], fold["train_start"], fold["train_signal_end"])
            keep, filtering = context["eligibility"].mask(raw.index, context["intervals"],
                cutoff=fold["data_cutoff"], labels=raw["label"].iloc[:, 0])
            filtered = raw.loc[keep].copy(); del raw
            if filtered.empty:
                raise ValueError("No legal mature H5 training rows remain")
            _verify_raw_labels(filtered, source, calendar)
            models, processors_by_key, records_by_key = {}, {}, {}
            for lookback in sorted({CANDIDATES[key]["lookback_calendar_years"] for key in plan["candidates"]}, reverse=True):
                members = [key for key in plan["candidates"] if CANDIDATES[key]["lookback_calendar_years"] == lookback]
                member_fold = candidate_fold(fold, members[0])
                dates = pd.DatetimeIndex(filtered.index.get_level_values("datetime"))
                panel = filtered.loc[dates >= pd.Timestamp(member_fold["train_start"])].copy()
                learned, processors = prepare_candidate_panel(panel, member_fold, calendar)
                maturity = validate_training_rows(learned, member_fold, calendar)
                if not primary.dated_membership_mask(learned.index, context["intervals"]).all():
                    raise ValueError("An out-of-signal-membership row reached model fitting")
                raw_feature_sha = feature_digest(panel["feature"])
                processed_sha = feature_digest(learned["feature"])
                label_sha = _label_digest(learned["label"].iloc[:, 0])
                index_bytes = _index_bytes(learned.index)
                shared = target/f"panel_{lookback}y_{year}"; shared.mkdir()
                (shared/"training_index.csv").write_bytes(index_bytes)
                primary._write(shared/"processors.json", processors)
                panel_proof = {**member_fold, **maturity, "lookback_calendar_years": lookback,
                    "training_index_sha256": hashlib.sha256(index_bytes).hexdigest(),
                    "raw_features_sha256": raw_feature_sha, "processed_features_sha256": processed_sha,
                    "ranked_label_sha256": label_sha, "processors_sha256": primary._sha(shared/"processors.json"),
                    "feature_processor_fit_count": 1, "training_filtering_full_five_year_panel": filtering}
                primary._write(shared/"panel.json", panel_proof)
                for key in members:
                    spec = FixedQlibLGBSpec(**CANDIDATES[key]["spec"])
                    view = ConsumedPanel(learned, processed_sha, label_sha)
                    if progress:
                        progress({"phase": "fit", "candidate": key, "execution_year": year, **maturity})
                    model = fit_fixed_qlib_lightgbm(view, spec=spec)
                    if (view.consume_count < 1 or view.consumed_features != processed_sha
                            or view.consumed_labels != label_sha or feature_digest(learned["feature"]) != processed_sha
                            or model.booster.current_iteration() != 300 or model.booster.num_trees() != 300):
                        raise ValueError("The native trainer did not consume the frozen panel for exactly 300 trees")
                    folder = target/f"{key}_{year}"; folder.mkdir()
                    shutil.copyfile(shared/"processors.json", folder/"processors.json")
                    fitted_at = datetime.now(timezone.utc).isoformat()
                    model.save_native(folder/"model", metadata={"model": key, "execution_year": year,
                        "label": LABEL, "fitted_at": fitted_at, "train_signal_end": member_fold["train_signal_end"],
                        "fit_role": "retrospective_staged_model_debug_not_historic_frozen_model",
                        "declaration_sha256": inputs["declaration"]["sha256"],
                        "fitted_inference_processors": processors["processors"],
                        "training_index_sha256": panel_proof["training_index_sha256"],
                        "consumed_training_features_sha256": processed_sha,
                        "consumed_ranked_label_sha256": label_sha})
                    importance = pd.DataFrame({"feature": model.feature_names,
                        "gain": model.booster.feature_importance(importance_type="gain"),
                        "split": model.booster.feature_importance(importance_type="split")})
                    importance.to_csv(folder/"feature_importance.csv", index=False, float_format="%.17g")
                    train_scores = pd.Series(model.booster.predict(learned["feature"], num_threads=1), index=learned.index)
                    train_stats = _save_diagnostics(folder, "training_diagnostics", score_diagnostics(
                        train_scores, learned["label"].iloc[:, 0], role="training_in_sample"))
                    record = {**member_fold, **maturity, "candidate": key, "model": key,
                        "spec": asdict(spec), "lookback_calendar_years": lookback,
                        "fitted_at": fitted_at, "model_sha256": primary._sha(folder/"model/model.txt"),
                        "actual_tree_count": model.booster.num_trees(), "panel": panel_proof,
                        "consumed_training_features_sha256": view.consumed_features,
                        "consumed_ranked_label_sha256": view.consumed_labels,
                        "training_diagnostics": train_stats, "historical_model_freeze_claim": False}
                    models[key], processors_by_key[key], records_by_key[key] = model, processors, record
                    del view, train_scores
                del panel, learned
                gc.collect()
            del filtered
            gc.collect()
            if progress:
                progress({"phase": "load_inference_features", "execution_year": year})
            raw = primary._raw_alpha158(source, calendar, context["intervals"], fold["signal_start"], fold["signal_end"])
            keep, inference_filter = context["eligibility"].mask(raw.index, context["intervals"])
            features = raw.loc[keep, "feature"].copy()
            inference_sha = feature_digest(features)
            valid_label, label_audit = context["eligibility"].mask(raw.index, context["intervals"],
                cutoff=fold["signal_end"], labels=raw["label"].iloc[:, 0])
            diagnostics_labels = raw.loc[valid_label, "label"].iloc[:, 0].copy()
            del raw
            for key in plan["candidates"]:
                folder, model = target/f"{key}_{year}", models[key]
                scores = apply_frozen_processors(features, model, processors_by_key[key]["processors"])
                if feature_digest(features) != inference_sha:
                    raise ValueError("A candidate changed the shared inference feature panel")
                stats = _save_diagnostics(folder, "validation_diagnostics", score_diagnostics(
                    scores.reindex(diagnostics_labels.index), diagnostics_labels,
                    role=f"{phase}_chronological_annual_prediction"))
                output = scores.rename("score").reset_index()
                output["datetime"] = pd.to_datetime(output["datetime"]).dt.strftime("%Y-%m-%d")
                output["model"], output["execution_year"] = key, year
                output = output[["model", "execution_year", "datetime", "instrument", "score"]]
                output.to_csv(folder/"predictions.csv", index=False, float_format="%.17g")
                all_scores.append(output)
                record = records_by_key[key]
                record.update(inference_filtering=inference_filter, inference_feature_sha256=inference_sha,
                    prediction_rows=len(output), inference_future_label_filter_applied=False,
                    validation_diagnostics=stats, diagnostic_label_filtering=label_audit)
                primary._write(folder/"protocol.json", record)
                primary._write(folder/"manifest.json", {"files": {str(path.relative_to(folder)): primary._sha(path)
                    for path in sorted(folder.rglob("*")) if path.is_file()}})
                records.append(record)
                if progress:
                    progress({"phase": "candidate_complete", "candidate": key, "execution_year": year,
                        "prediction_rows": len(output), "training_rank_ic": record["training_diagnostics"]["mean_daily_rank_ic"],
                        "validation_rank_ic": stats["mean_daily_rank_ic"]})
            del features, models
            gc.collect()
        predictions = pd.concat(all_scores, ignore_index=True).sort_values(["model", "execution_year", "datetime", "instrument"])
        if predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
            raise ValueError("Duplicate candidate/year/signal/security prediction")
        _assert_proofs(inputs)
        for context in contexts.values():
            context["scope"].assert_unchanged()
            primary.assert_market_originals_unchanged(context["audit"], context["stock_basic"])
            if any(primary._sha(context["source"].dataset_dir/name) != digest for name, digest in context["source_files"].items()):
                raise ValueError("Consumed source feature bytes changed during model debugging")
        if implementation_sha256() != implementation or len(records) != plan["native_fit_count"]:
            raise ValueError("Model-debug implementation or actual fit budget changed")
        predictions.to_csv(target/"predictions.csv", index=False, float_format="%.17g")
        manifest = {"schema_version": 1, "kind": PACKAGE_KIND, "phase": phase,
            "started_at": started, "completed_at": datetime.now(timezone.utc).isoformat(),
            "inputs": inputs, "models": plan["candidates"], "execution_years": plan["years"],
            "folds": records, "baseline_reuse": baseline, "prediction_rows": len(predictions),
            "native_fit_count": len(records), "feature_processor_fit_count": sum(
                len({record["lookback_calendar_years"] for record in records if record["execution_year"] == year})
                for year in plan["years"]),
            "candidate_grid_expansion": False, "early_stopping": False,
            "predeclared_candidate_comparison": True, "candidate_configuration_count": 3,
            "candidate_selection_year": 2023, "adaptive_hyperparameter_search": False,
            "single_native_fit_parameter_search": False,
            "selection_performed_here": False, "active_paper_changed": False,
            "historical_model_freeze_claim": False, "new_future_out_of_sample_claim": False,
            "implementation_sha256": implementation,
            "source_families": {name: {"source_snapshot": str(context["source"].path),
                "source_manifest_sha256": context["source"].manifest_sha256,
                "scope_manifest_sha256": context["scope_manifest_sha256"]} for name, context in contexts.items()},
            "files": {str(path.relative_to(target)): primary._sha(path) for path in sorted(target.rglob("*")) if path.is_file()}}
        primary._write(target/"manifest.json", manifest)
        return {"manifest": manifest, "predictions": predictions, "path": target}
    except Exception as exc:
        primary._write(target/"failure.json", {"exception_type": type(exc).__name__, "message": str(exc),
            "completed_native_model_records": len(records), "no_completed_package_claim": True})
        raise


def verify_debug_model_package(directory, declaration_path):
    """Check saved native models and sources without fitting or predicting."""
    root = Path(directory).resolve()
    declaration, _ = _load_declaration(declaration_path)
    declaration["_self_proof"] = _proof(declaration_path)
    manifest = _read(root/"manifest.json")
    if (manifest.get("kind") != PACKAGE_KIND or manifest.get("inputs", {}).get("declaration") != _proof(declaration_path)
            or manifest.get("implementation_sha256") != implementation_sha256()):
        raise ValueError("Model-debug package differs from its frozen declaration")
    _verify_files(root, manifest["files"])
    _assert_proofs(manifest["inputs"])
    selection = manifest["inputs"].get("selection_decision", {}).get("path")
    plan = phase_plan(declaration, phase=manifest["phase"], selection_path=selection)
    if (manifest.get("models") != plan["candidates"] or manifest.get("execution_years") != plan["years"]
            or manifest.get("native_fit_count") != plan["native_fit_count"]
            or len(manifest.get("folds", [])) != plan["native_fit_count"]):
        raise ValueError("Saved package differs from its exact staged fit budget")
    expected_records = {(key, year) for year in plan["years"] for key in plan["candidates"]}
    if {(record["candidate"], record["execution_year"]) for record in manifest["folds"]} != expected_records:
        raise ValueError("Saved model package has duplicated, missing or later-stage candidate folds")
    columns = ["model", "execution_year", "datetime", "instrument", "score"]
    combined = pd.read_csv(root/"predictions.csv", dtype=str, keep_default_na=False)
    if (combined.columns.tolist() != columns or len(combined) != manifest.get("prediction_rows")
            or set(combined.model) != set(plan["candidates"])
            or set(combined.execution_year) != {str(year) for year in plan["years"]}
            or combined.duplicated(columns[:-1]).any()
            or not np.isfinite(pd.to_numeric(combined.score, errors="raise")).all()):
        raise ValueError("Saved combined candidate predictions are incomplete or invalid")
    for record in manifest["folds"]:
        folder = root/f"{record['candidate']}_{record['execution_year']}"
        model = load_fixed_qlib_model(folder/"model")
        processors = _read(folder/"processors.json")
        scores = pd.read_csv(folder/"predictions.csv", dtype=str, keep_default_na=False)
        matching = combined.loc[combined.model.eq(record["candidate"])
            & combined.execution_year.eq(str(record["execution_year"]))]
        if (scores.columns.tolist() != columns or len(scores) != record["prediction_rows"]
                or not scores.sort_values(columns[:-1]).reset_index(drop=True).equals(
                    matching.sort_values(columns[:-1]).reset_index(drop=True))
                or not scores.datetime.between(record["signal_start"], record["signal_end"]).all()
                or _read(folder/"protocol.json") != record):
            raise ValueError("Combined predictions differ from their complete annual candidate records")
        if (asdict(model.spec) != CANDIDATES[record["candidate"]]["spec"]
                or asdict(model.spec) != record["spec"]
                or model.booster.num_trees() != 300 or model.booster.current_iteration() != 300
                or model.training_rows != record["training_rows"]
                or primary._sha(folder/"model/model.txt") != record["model_sha256"]
                or model.metadata.get("model") != record["candidate"]
                or model.metadata.get("execution_year") != record["execution_year"]
                or model.metadata.get("declaration_sha256") != manifest["inputs"]["declaration"]["sha256"]
                or model.metadata.get("fitted_inference_processors") != processors.get("processors")
                or model.feature_names != processors.get("feature_names")
                or record["consumed_training_features_sha256"] != record["panel"]["processed_features_sha256"]
                or record["consumed_ranked_label_sha256"] != record["panel"]["ranked_label_sha256"]):
            raise ValueError("Native candidate spec, 300 trees or actual consumed panel proof differs")
        family = "primary" if record["execution_year"] <= 2024 else "stability"
        old_manifest = _read(declaration["source_families"][family]["model_manifest"]["path"])
        original = next((fold for fold in old_manifest["folds"] if fold["execution_year"] == record["execution_year"]), None)
        if original is None:
            raise ValueError("Candidate annual fold is outside its bound A5 source family")
        expected = candidate_fold(original, record["candidate"])
        if any(record[key] != expected[key] for key in ("train_start", "data_cutoff", "train_signal_end",
                "signal_start", "signal_end", "first_execution_session")):
            raise ValueError("Candidate chronology differs from the bound actual-session A5 fold")
    for reuse in manifest["baseline_reuse"]:
        for key in ("original_predictions", "copied_predictions", "original_native_model", "original_native_manifest", "original_processors"):
            _assert_proof(reuse[key])
        if reuse["original_predictions"]["sha256"] != reuse["copied_predictions"]["sha256"]:
            raise ValueError("Copied baseline predictions differ from their original bytes")
    return manifest
