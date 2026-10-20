"""Reuse fixed model bytes when only the separately bound execution code changes.

No model is loaded for fitting or prediction. Original native metadata and fit
timestamps retain their original training declaration; the new top-level
declaration is an execution binding, never a claim that the models were refit.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil

import numpy as np
import pandas as pd

from stopat30m import ashare_stability_research as research
from stopat30m.ashare_union_validation import execution_year_scores
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot


_SHA = re.compile(r"[0-9a-f]{64}")
_ALLOWED_PROTOCOL_CHANGES = {"declared_at", "execution_implementations"}


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": _sha(path)}


def _read(path):
    value = json.loads(Path(path).read_bytes())
    if not isinstance(value, dict):
        raise ValueError("A frozen JSON object is required")
    return value


def _write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
                               indent=2, allow_nan=False) + "\n")


def _relative(root, name):
    if not isinstance(name, str) or Path(name).is_absolute() or ".." in Path(name).parts:
        raise ValueError("Artifact manifest paths must remain inside their package")
    path = root / name
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("Missing or unsafe frozen artifact")
    return path


def _verify_files(root, files, *, manifest_name="manifest.json"):
    if not isinstance(files, dict) or not files:
        raise ValueError("A complete frozen artifact manifest is required")
    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("Frozen model packages cannot contain symbolic links")
        if path.is_file() and path != root / manifest_name:
            actual.add(path.relative_to(root).as_posix())
    if actual != set(files):
        raise ValueError("Frozen artifact manifest omits or invents package files")
    for name, digest in files.items():
        if not isinstance(digest, str) or not _SHA.fullmatch(digest) or _sha(_relative(root, name)) != digest:
            raise ValueError("Frozen model/prediction artifact bytes changed")


def _verify_proofs(proofs):
    seen = set()
    for proof in proofs.values():
        if (not isinstance(proof, dict) or not isinstance(proof.get("path"), str)
                or not Path(proof["path"]).is_absolute()
                or not isinstance(proof.get("sha256"), str) or not _SHA.fullmatch(proof["sha256"])):
            raise ValueError("Exact absolute original source proofs are required")
        key = proof["path"], proof["sha256"]
        if key not in seen and _sha(proof["path"]) != proof["sha256"]:
            raise ValueError("Bound original source bytes changed")
        seen.add(key)


def _protocols(old, new):
    old_stable = {key: value for key, value in old.items() if key not in _ALLOWED_PROTOCOL_CHANGES}
    new_stable = {key: value for key, value in new.items() if key not in _ALLOWED_PROTOCOL_CHANGES}
    if old_stable != new_stable:
        raise ValueError("Reuse permits only declaration time and execution implementation changes")
    for value in (old, new):
        if value.get("kind") != "a5_union_stability_fixed_research_protocol":
            raise ValueError("Only the separate fixed stability protocol can reuse these models")
        if not value.get("execution_implementations"):
            raise ValueError("Separate bound execution implementations are required")
    times = [pd.Timestamp(value["declared_at"]) for value in (old, new)]
    if any(pd.isna(t) or t.tzinfo is None for t in times) or times[1] < times[0]:
        raise ValueError("The execution declaration cannot precede the original training declaration")
    if old["execution_implementations"] == new["execution_implementations"]:
        raise ValueError("A separately declared execution implementation change is required")
    # Prior execution-code paths may now contain the new implementation. Their
    # old SHA remains historical evidence, not a claim about current code bytes.
    _verify_proofs(new["execution_implementations"])
    _verify_proofs(new["execution_rules"])


def _scope_directory(manifest, scope_sha):
    proofs = manifest.get("scope_sources", {})
    if not proofs:
        raise ValueError("Frozen historical-scope source proofs are required")
    _verify_proofs(proofs)
    candidates = {str(Path(item["path"]).resolve().parent) for item in proofs.values()
                  if Path(item["path"]).name == "manifest.json" and item["sha256"] == scope_sha}
    if len(candidates) != 1:
        raise ValueError("Model package must bind one exact historical-scope manifest")
    return Path(candidates.pop())


def _source_files(folder, source):
    files = _read(folder / "source_files.json")
    if not files or "calendars/day.txt" not in files:
        raise ValueError("Model package lacks its exact consumed source inventory")
    for name, digest in files.items():
        path = _relative(source.dataset_dir, name)
        if (not isinstance(digest, str) or not _SHA.fullmatch(digest)
                or source.manifest["files"].get(name, {}).get("sha256") != digest or _sha(path) != digest):
            raise ValueError("Consumed training/inference source bytes changed")


def _folds_and_predictions(folder, manifest, protocol, calendar, original_declaration_sha):
    plan = research.union_fold_plan(calendar, [2025, 2026],
                                    evaluation_end=protocol["training"]["evaluation_end"])
    records = manifest.get("folds", [])
    if len(records) != 2 or [record.get("execution_year") for record in records] != [2025, 2026]:
        raise ValueError("Both original annual model folds must be preserved")
    columns = ["model", "execution_year", "datetime", "instrument", "score"]
    combined = pd.read_csv(folder / "predictions.csv", dtype=str, keep_default_na=False)
    if (combined.columns.tolist() != columns or set(combined.model) != {"LGB"}
            or combined.duplicated(columns[:-1]).any() or len(combined) != manifest.get("prediction_rows")
            or set(combined.execution_year) != {"2025", "2026"}
            or not np.isfinite(pd.to_numeric(combined.score, errors="raise")).all()):
        raise ValueError("Frozen combined annual-fold predictions are incomplete or invalid")
    for record, fold in zip(records, plan):
        year, directory = fold["execution_year"], folder / f"LGB_{fold['execution_year']}"
        if any(record.get(name) != value for name, value in fold.items()):
            raise ValueError("Original training/label fold chronology differs from the fixed policy")
        original_fold = _read(directory / "protocol.json")
        if original_fold != record or not record.get("fitted_at") or record.get("historical_model_freeze_claim") is not False:
            raise ValueError("Original model fold metadata or fit timestamp changed")
        _verify_files(directory, _read(directory / "manifest.json")["files"])
        native = _read(directory / "model/manifest.json")
        unsigned = {key: value for key, value in native.items() if key != "manifest_sha256"}
        native_sha = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        processors = _read(directory / "processors.json")
        metadata = native.get("metadata", {})
        if (native.get("manifest_sha256") != native_sha
                or native.get("kind") != "qlib_alpha158_fixed_lightgbm"
                or native.get("model_sha256") != _sha(directory / "model/model.txt")
                or record.get("model_sha256") != native["model_sha256"]
                or native.get("model_spec") != protocol["training"]["fixed_lightgbm_spec"]
                or native.get("train_params") != research.FixedQlibLGBSpec().params()
                or native.get("training_implementation_sha256") != manifest["implementation_sha256"]["native_trainer"]
                or native.get("early_stopping") is not False or native.get("parameter_search") is not False
                or native.get("training_rows") != record.get("training_rows")
                or metadata.get("declaration_sha256") != original_declaration_sha
                or metadata.get("execution_year") != year or metadata.get("model") != "LGB"
                or metadata.get("train_signal_end") != fold["train_signal_end"]
                or metadata.get("fit_role") != "retrospective_reconstructed_history"
                or metadata.get("fitted_inference_processors") != processors.get("processors")
                or len(native.get("feature_names", [])) != 158
                or len(set(native.get("feature_names", []))) != 158
                or native.get("feature_names") != processors.get("feature_names")):
            raise ValueError("Original native model, processors or original training declaration differs")
        scores = pd.read_csv(directory / "predictions.csv", dtype=str, keep_default_na=False)
        selected = combined.loc[combined.execution_year.eq(str(year))].reset_index(drop=True)
        order = columns[:-1]
        if (scores.columns.tolist() != columns
                or not selected.sort_values(order).reset_index(drop=True).equals(scores.sort_values(order).reset_index(drop=True))
                or not scores.datetime.between(fold["signal_start"], fold["signal_end"]).all()):
            raise ValueError("Combined predictions differ from their original annual-fold CSV bytes/rows")
    # Exercise the same next-actual-session year routing used by execution;
    # this does not regenerate predictions or compute any strategy return.
    execution_year_scores(combined, calendar)


def reuse_fixed_stability_models(original_models_dir, original_protocol_path, new_protocol_path,
                                market_audit_path, output_dir, *, progress=None):
    """Copy verified fixed artifacts under a new execution-only declaration.

    Returns ``{"path": Path, "manifest": dict}``. Existing outputs and partial
    copies are preserved. Only top-level manifest/declaration are rebound;
    every native model, fold metadata, processor, CSV and training timestamp
    remains byte-identical, with the original declaration archived locally.
    """
    original = Path(original_models_dir).resolve()
    target = guard_qlib_output_path(output_dir)
    if target.exists() or target == original or target.is_relative_to(original) or original.is_relative_to(target):
        raise ValueError("Preserve original models and every prior reuse output")
    old_path, new_path, audit_path = [Path(path).resolve() for path in
                                    (original_protocol_path, new_protocol_path, market_audit_path)]
    if old_path == new_path or old_path.is_relative_to(target) or new_path.is_relative_to(target):
        raise ValueError("Old and new immutable declarations must be distinct external files")
    bound = {"original_manifest": _proof(original / "manifest.json"), "original_protocol": _proof(old_path),
             "new_execution_protocol": _proof(new_path), "market_audit": _proof(audit_path),
             "reuse_implementation": _proof(__file__)}
    manifest, old, new, audit = [_read(path) for path in
                                (original / "manifest.json", old_path, new_path, audit_path)]
    _protocols(old, new)
    _verify_files(original, manifest.get("files"))
    if (manifest.get("kind") != "a5_union_stability_fixed_predictions" or manifest.get("schema_version") != 1
            or manifest.get("policy") != research.POLICY or manifest.get("models") != ["LGB"]
            or manifest.get("hyperparameter_search") is not False or manifest.get("early_stopping") is not False
            or manifest.get("selection_performed") is not False or manifest.get("active_paper_changed") is not False
            or manifest.get("historical_model_freeze_claim") is not False
            or manifest.get("new_future_out_of_sample_claim") is not False or "model_reuse" in manifest
            or manifest.get("inputs", {}).get("declaration") != _proof(old_path)
            or _sha(original / "declaration.json") != _sha(old_path)):
        raise ValueError("Only an intact original fixed training generation may be reused")
    required = {"predictions.csv", "source_files.json", "membership_audit.json", "union_training_implementation.py",
                "declaration.json", *{f"LGB_{year}/{name}" for year in (2025, 2026) for name in
                ("manifest.json", "protocol.json", "predictions.csv", "processors.json", "model/manifest.json", "model/model.txt")}}
    if not required.issubset(manifest["files"]):
        raise ValueError("Original model package lacks required native, source, prediction or chronology artifacts")
    implementation = research.implementation_sha256()
    if (manifest.get("implementation_sha256") != implementation or old.get("implementation_sha256") != implementation
            or new.get("implementation_sha256") != implementation
            or _sha(original / "union_training_implementation.py") != implementation["stability_training"]):
        raise ValueError("Training implementation changed; retraining is required instead of reuse")
    _verify_proofs(manifest["inputs"])
    if manifest["inputs"].get("market_audit") != _proof(audit_path):
        raise ValueError("Model reuse cannot replace its original source audit")
    source = load_qlib_snapshot(manifest["source_snapshot"], verify=True)
    if (source.manifest_sha256 != manifest.get("source_manifest_sha256")
            or source.manifest_sha256 != old.get("source_snapshot_manifest_sha256")):
        raise ValueError("Original training/inference source generation changed")
    scope = _scope_directory(manifest, old["historical_scope_manifest_sha256"])
    research._load_protocol(old_path, source, scope)
    research._load_protocol(new_path, source, scope)
    calendar = (source.dataset_dir / "calendars/day.txt").read_text().splitlines()
    folds = research.union_fold_plan(calendar, [2025, 2026], evaluation_end=old["training"]["evaluation_end"])
    basic = Path(audit["stock_basic_path"]).resolve()
    if manifest["inputs"].get("stock_basic") != _proof(basic):
        raise ValueError("Frozen model stock identity input differs from the unchanged source audit")
    research.validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=_sha(scope / "manifest.json"), stock_basic_sha=_sha(basic),
        coverage_start=folds[0]["first_execution_session"], coverage_end=old["training"]["evaluation_end"])
    research.assert_market_originals_unchanged(audit, basic)
    _source_files(original, source)
    membership = _read(original / "membership_audit.json")
    if membership.get("status") != "passed":
        raise ValueError("Original dated-membership training audit did not pass")
    _verify_proofs(membership["bound_inputs"])
    _folds_and_predictions(original, manifest, old, calendar, _sha(old_path))
    _verify_proofs(bound)
    if progress:
        progress({"phase": "copy_verified_frozen_models_without_refit", "prediction_rows": manifest["prediction_rows"]})
    target.mkdir(parents=True, exist_ok=False)
    for name in manifest["files"]:
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_relative(original, name), destination)
    archive = target / "reuse_provenance"
    archive.mkdir()
    shutil.copyfile(original / "manifest.json", archive / "original_model_manifest.json")
    shutil.copyfile(original / "declaration.json", archive / "original_training_declaration.json")
    shutil.copyfile(__file__, archive / "reuse_implementation.py")
    shutil.copyfile(new_path, target / "declaration.json")
    preserved = {name: digest for name, digest in manifest["files"].items() if name != "declaration.json"}
    for name, digest in preserved.items():
        if _sha(target / name) != digest:
            raise ValueError("An original model/prediction/timestamp artifact changed during copying")
    result = deepcopy(manifest)
    result["inputs"]["declaration"] = _proof(new_path)
    result["inputs"]["original_training_declaration"] = _proof(archive / "original_training_declaration.json")
    result["model_reuse"] = {"kind": "a5_execution_only_declaration_model_reuse",
        "reused_at": datetime.now(timezone.utc).isoformat(), "training_reperformed": False,
        "predictions_regenerated": False, "original_fit_timestamps_preserved": True,
        "native_metadata_retains_original_training_declaration": True,
        "new_execution_protocol_was_not_used_for_original_model_fitting": True,
        "allowed_protocol_differences": sorted(_ALLOWED_PROTOCOL_CHANGES),
        "original_models_directory": str(original), "bound_inputs": bound,
        "original_manifest_archive": _proof(archive / "original_model_manifest.json"),
        "original_training_declaration_archive": _proof(archive / "original_training_declaration.json"),
        "reuse_implementation_archive": _proof(archive / "reuse_implementation.py"),
        "original_training_protocol_sha256": _sha(old_path), "new_execution_protocol_sha256": _sha(new_path),
        "unchanged_model_prediction_and_metadata_files": preserved,
        "source_market_audit_unchanged": manifest["inputs"]["market_audit"],
        "prior_execution_hashes_are_historical_not_asserted_to_match_current_execution": True}
    # Recheck all source proofs and actual consumed feature bytes before final publication.
    _verify_files(original, manifest["files"])
    _verify_proofs(bound)
    _verify_proofs(manifest["scope_sources"])
    research.assert_market_originals_unchanged(audit, basic)
    if load_qlib_snapshot(source.path, verify=True).manifest_sha256 != source.manifest_sha256:
        raise ValueError("Source generation changed during frozen model reuse")
    _source_files(original, source)
    if research.implementation_sha256() != implementation:
        raise ValueError("Training implementation changed during frozen model reuse")
    _verify_proofs(new["execution_implementations"])
    _verify_proofs(bound)
    _verify_files(original, manifest["files"])
    result["files"] = {path.relative_to(target).as_posix(): _sha(path)
                       for path in sorted(target.rglob("*")) if path.is_file()}
    _verify_files(target, result["files"])
    _write(target / "manifest.json", result)
    return {"path": target, "manifest": result}


def verify_reused_stability_models(models_dir, protocol_path, market_audit_path):
    """Revalidate reuse provenance and original sources before/after execution.

    Unlike checking only the copied prediction files, this follows the bound
    original package, original training declaration, exact unchanged market
    audit/scope/features, and the new execution declaration. The returned
    proof receipt may be retained by a fee-only runner; call this function
    again at completion rather than treating a cached ``passed`` as evidence.
    """
    target = Path(models_dir).resolve()
    manifest = _read(target / "manifest.json")
    _verify_files(target, manifest.get("files"))
    reuse = manifest.get("model_reuse", {})
    if (manifest.get("kind") != "a5_union_stability_fixed_predictions"
            or reuse.get("kind") != "a5_execution_only_declaration_model_reuse"
            or reuse.get("training_reperformed") is not False or reuse.get("predictions_regenerated") is not False
            or reuse.get("original_fit_timestamps_preserved") is not True
            or reuse.get("native_metadata_retains_original_training_declaration") is not True
            or reuse.get("new_execution_protocol_was_not_used_for_original_model_fitting") is not True
            or reuse.get("allowed_protocol_differences") != sorted(_ALLOWED_PROTOCOL_CHANGES)):
        raise ValueError("An explicit execution-only frozen model reuse package is required")
    bound = reuse["bound_inputs"]
    _verify_proofs(bound)
    original = Path(reuse["original_models_directory"]).resolve()
    if target == original or bound.get("original_manifest") != _proof(original / "manifest.json"):
        raise ValueError("The original model package must remain separately bound")
    old_manifest = _read(original / "manifest.json")
    _verify_files(original, old_manifest["files"])
    old_path = Path(bound["original_protocol"]["path"])
    new_path, audit_path = Path(protocol_path).resolve(), Path(market_audit_path).resolve()
    if (bound["new_execution_protocol"] != _proof(new_path) or bound["market_audit"] != _proof(audit_path)
            or bound["reuse_implementation"] != _proof(__file__)):
        raise ValueError("Reuse declaration, audit or reuse implementation changed")
    old, new = _read(old_path), _read(new_path)
    _protocols(old, new)
    archive = target / "reuse_provenance"
    archived = {"original_manifest_archive": _proof(archive / "original_model_manifest.json"),
        "original_training_declaration_archive": _proof(archive / "original_training_declaration.json"),
        "reuse_implementation_archive": _proof(archive / "reuse_implementation.py")}
    if (any(reuse.get(name) != item for name, item in archived.items())
            or archived["original_manifest_archive"]["sha256"] != bound["original_manifest"]["sha256"]
            or archived["original_training_declaration_archive"]["sha256"] != _sha(old_path)
            or archived["reuse_implementation_archive"]["sha256"] != _sha(__file__)
            or _sha(original / "declaration.json") != _sha(old_path)
            or _sha(target / "declaration.json") != _sha(new_path)
            or reuse.get("original_training_protocol_sha256") != _sha(old_path)
            or reuse.get("new_execution_protocol_sha256") != _sha(new_path)):
        raise ValueError("Archived original declaration/model provenance differs")
    preserved = {name: digest for name, digest in old_manifest["files"].items() if name != "declaration.json"}
    if reuse.get("unchanged_model_prediction_and_metadata_files") != preserved:
        raise ValueError("Reuse must account for every original model/prediction/metadata artifact")
    expected_files = set(old_manifest["files"]) | {
        "reuse_provenance/original_model_manifest.json", "reuse_provenance/original_training_declaration.json",
        "reuse_provenance/reuse_implementation.py"}
    if set(manifest["files"]) != expected_files or any(manifest["files"].get(name) != digest for name, digest in preserved.items()):
        raise ValueError("Reused model, prediction or original timestamp bytes differ")
    omitted = {"files", "inputs", "model_reuse"}
    if ({key: value for key, value in manifest.items() if key not in omitted}
            != {key: value for key, value in old_manifest.items() if key not in omitted}):
        raise ValueError("Reused training history, fold metadata or fit timestamps were rewritten")
    expected_inputs = {**old_manifest["inputs"], "declaration": _proof(new_path),
                       "original_training_declaration": archived["original_training_declaration_archive"]}
    if (manifest["inputs"] != expected_inputs or old_manifest["inputs"]["declaration"] != _proof(old_path)
            or old_manifest["inputs"]["market_audit"] != _proof(audit_path)
            or reuse.get("source_market_audit_unchanged") != old_manifest["inputs"]["market_audit"]):
        raise ValueError("The original training sources or execution declaration binding differ")
    _verify_proofs(manifest["inputs"])
    implementation = research.implementation_sha256()
    if (old_manifest["implementation_sha256"] != implementation or old["implementation_sha256"] != implementation
            or new["implementation_sha256"] != implementation
            or _sha(target / "union_training_implementation.py") != implementation["stability_training"]):
        raise ValueError("Original training implementation changed")
    source = load_qlib_snapshot(manifest["source_snapshot"], verify=True)
    if source.manifest_sha256 != old["source_snapshot_manifest_sha256"] or source.manifest_sha256 != manifest["source_manifest_sha256"]:
        raise ValueError("Reused training source generation changed")
    scope = _scope_directory(manifest, old["historical_scope_manifest_sha256"])
    research._load_protocol(old_path, source, scope)
    research._load_protocol(new_path, source, scope)
    calendar = (source.dataset_dir / "calendars/day.txt").read_text().splitlines()
    folds = research.union_fold_plan(calendar, [2025, 2026], evaluation_end=old["training"]["evaluation_end"])
    audit = _read(audit_path)
    basic = Path(audit["stock_basic_path"])
    if manifest["inputs"]["stock_basic"] != _proof(basic):
        raise ValueError("Reused stock identity table differs from the original source audit")
    research.validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=_sha(scope / "manifest.json"), stock_basic_sha=_sha(basic),
        coverage_start=folds[0]["first_execution_session"], coverage_end=old["training"]["evaluation_end"])
    research.assert_market_originals_unchanged(audit, basic)
    _source_files(target, source)
    _folds_and_predictions(target, manifest, old, calendar, _sha(old_path))
    if research.implementation_sha256() != implementation:
        raise ValueError("Training implementation changed during reuse verification")
    _verify_proofs(bound)
    _verify_proofs(new["execution_implementations"])
    _verify_proofs(new["execution_rules"])
    _verify_proofs(manifest["scope_sources"])
    research.assert_market_originals_unchanged(audit, basic)
    _verify_files(original, old_manifest["files"])
    _verify_files(target, manifest["files"])
    return {"kind": "a5_execution_only_model_reuse_verification", "status": "passed",
        "verified_at": datetime.now(timezone.utc).isoformat(), "model_manifest": _proof(target / "manifest.json"),
        "bound_inputs": {**bound, **archived, "source_snapshot_manifest": _proof(source.path / "manifest.json"),
                         "historical_scope_manifest": _proof(scope / "manifest.json")},
        "unchanged_model_prediction_metadata_file_count": len(preserved),
        "prediction_rows": manifest["prediction_rows"], "training_reperformed": False,
        "original_fit_timestamps_preserved": True}
