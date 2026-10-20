"""Small synthetic frozen packages; never fit a model or compute performance."""
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_stability_model_reuse as reuse


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def native_sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def seal(folder):
    write(folder / "manifest.json", {"schema_version": 1, "files": {
        path.relative_to(folder).as_posix(): reuse._sha(path)
        for path in folder.rglob("*") if path.is_file() and path != folder / "manifest.json"}})


def package(tmp_path, monkeypatch):
    """Only the snapshot loader is replaced; all proof/clock/copy guards run."""
    source_root, scope, models = [tmp_path / name for name in ("source", "scope", "models-v1")]
    dataset = source_root / "dataset"
    (dataset / "calendars").mkdir(parents=True)
    calendar = pd.bdate_range("2019-10-01", "2026-10-20").strftime("%Y-%m-%d").tolist()
    (dataset / "calendars/day.txt").write_text("\n".join(calendar) + "\n")
    feature = dataset / "features/sh600000/close.day.bin"
    feature.parent.mkdir(parents=True)
    feature.write_bytes(b"explicit synthetic frozen feature buffer; not market observations")
    source_files = {str(path.relative_to(dataset)): reuse._sha(path) for path in (dataset / "calendars/day.txt", feature)}
    write(source_root / "manifest.json", {"synthetic_source_fixture": True})
    source = SimpleNamespace(path=source_root, dataset_dir=dataset,
        manifest_sha256=reuse._sha(source_root / "manifest.json"),
        manifest={"files": {name: {"sha256": digest} for name, digest in source_files.items()}})
    monkeypatch.setattr(reuse, "load_qlib_snapshot", lambda path, verify: source)
    write(scope / "manifest.json", {"synthetic_historical_scope_fixture": True})
    basic = tmp_path / "basic.csv"
    basic.write_text("code,ipoDate,outDate\nsh.600000,1999-11-10,\n")
    audit_path = tmp_path / "market_audit.json"
    write(audit_path, {"kind": "a5_stability_market_audit", "status": "passed", "source_preflight_only": False,
        "start": "2025-01-01", "end": "2026-10-19", "source_snapshot_manifest_sha256": source.manifest_sha256,
        "historical_scope_manifest_sha256": reuse._sha(scope / "manifest.json"),
        "stock_basic_path": str(basic), "stock_basic_sha256": reuse._sha(basic),
        "checks": {name: True for name in reuse.research.MARKET_CHECKS},
        "bound_inputs": {"stock_basic": reuse._proof(basic), "distribution_tax_contracts": reuse._proof(feature)}})
    old_code, new_code, rules = [tmp_path / name for name in ("old_execution.py", "new_execution.py", "rules.json")]
    old_code.write_text("# synthetic old execution implementation\n")
    new_code.write_text("# synthetic corrected execution implementation\n")
    write(rules, {"same_fixed_rules": True})
    implementation = reuse.research.implementation_sha256()
    protocol = {"schema_version": 1, "kind": "a5_union_stability_fixed_research_protocol",
        "declared_at": "2026-10-20T06:00:00+00:00",
        "source_snapshot_manifest_sha256": source.manifest_sha256,
        "historical_scope_manifest_sha256": reuse._sha(scope / "manifest.json"),
        "implementation_sha256": implementation, "models": ["MOM60", "LGB"],
        "execution": {"unchanged_fixture_cost": "5"}, "validity": {"unchanged": True},
        "execution_implementations": {"adapter": reuse._proof(old_code)},
        "execution_rules": {"rules": reuse._proof(rules)},
        "periods": {"2025": ["2025-01-01", "2025-12-31"], "2026": ["2026-01-01", "2026-10-19"],
                    "continuous": ["2025-01-01", "2026-10-19"]},
        "training": {"execution_years": [2025, 2026], "evaluation_end": "2026-10-19",
            "lookback_calendar_years": 5, "purge_future_sessions": 6, "label": reuse.research.LABEL,
            "fixed_lightgbm_spec": asdict(reuse.research.FixedQlibLGBSpec()), "hyperparameter_search": False,
            "early_stopping": False, "membership_filter": "signal_date_only_no_future_membership_survival_condition"}}
    old_path, new_path = tmp_path / "protocol-v1.json", tmp_path / "protocol-v2.json"
    write(old_path, protocol)
    new = deepcopy(protocol)
    new.update(declared_at="2026-10-20T08:00:00+00:00", execution_implementations={"adapter": reuse._proof(new_code)})
    write(new_path, new)
    models.mkdir()
    (models / "declaration.json").write_bytes(old_path.read_bytes())
    (models / "union_training_implementation.py").write_bytes(Path(reuse.research.__file__).read_bytes())
    write(models / "source_files.json", source_files)
    scope_sources = {"official:manifest": reuse._proof(scope / "manifest.json")}
    write(models / "membership_audit.json", {"status": "passed", "bound_inputs": scope_sources})
    folds, predictions = [], []
    for fold in reuse.research.union_fold_plan(calendar, [2025, 2026], evaluation_end="2026-10-19"):
        year = fold["execution_year"]
        folder = models / f"LGB_{year}"
        (folder / "model").mkdir(parents=True)
        (folder / "model/model.txt").write_bytes(b"synthetic immutable native model fixture, never loaded for inference\n")
        processors = {"processors": [{"class": "synthetic_train_only_processor"}],
                      "feature_names": [f"FEATURE_{n}" for n in range(158)]}
        write(folder / "processors.json", processors)
        native = {"schema_version": 1, "kind": "qlib_alpha158_fixed_lightgbm",
            "model_sha256": reuse._sha(folder / "model/model.txt"),
            "model_spec": protocol["training"]["fixed_lightgbm_spec"],
            "train_params": reuse.research.FixedQlibLGBSpec().params(),
            "training_implementation_sha256": implementation["native_trainer"], "training_rows": 2,
            "early_stopping": False, "parameter_search": False, "feature_names": processors["feature_names"],
            "metadata": {"declaration_sha256": reuse._sha(old_path), "model": "LGB", "execution_year": year,
                "train_signal_end": fold["train_signal_end"], "fit_role": "retrospective_reconstructed_history",
                "fitted_inference_processors": processors["processors"]}}
        native["manifest_sha256"] = native_sha(native)
        write(folder / "model/manifest.json", native)
        record = {**fold, "training_rows": 2, "model_sha256": native["model_sha256"],
                  "fitted_at": f"2026-10-20T07:0{year - 2025}:00+00:00", "historical_model_freeze_claim": False}
        write(folder / "protocol.json", record)
        rows = [dict(model="LGB", execution_year=year, datetime=day, instrument="SH600000", score="0.01")
                for day in (fold["signal_start"], fold["first_execution_session"])]
        pd.DataFrame(rows).to_csv(folder / "predictions.csv", index=False)
        predictions.extend(rows)
        seal(folder)
        folds.append(record)
    pd.DataFrame(predictions).to_csv(models / "predictions.csv", index=False)
    seal(models)
    manifest = json.loads((models / "manifest.json").read_text())
    manifest.update(kind="a5_union_stability_fixed_predictions", policy=reuse.research.POLICY,
        models=["LGB"], hyperparameter_search=False, early_stopping=False,
        historical_model_freeze_claim=False, new_future_out_of_sample_claim=False,
        active_paper_changed=False, selection_performed=False, started_at="2026-10-20T06:30:00+00:00",
        completed_at="2026-10-20T07:30:00+00:00", implementation_sha256=implementation,
        source_snapshot=str(source_root), source_manifest_sha256=source.manifest_sha256,
        prediction_rows=len(predictions), scope_sources=scope_sources, folds=folds,
        inputs={"declaration": reuse._proof(old_path), "market_audit": reuse._proof(audit_path),
                "stock_basic": reuse._proof(basic)})
    write(models / "manifest.json", manifest)
    return SimpleNamespace(models=models, old=old_path, new=new_path, audit=audit_path,
        output=tmp_path / "models-v2", source=source, feature=feature, old_code=old_code)


def run(case):
    return reuse.reuse_fixed_stability_models(case.models, case.old, case.new, case.audit, case.output)


def test_only_execution_binding_changes_all_model_bytes_and_fit_times_remain_original(tmp_path, monkeypatch):
    case = package(tmp_path, monkeypatch)
    original = {p.relative_to(case.models).as_posix(): p.read_bytes() for p in case.models.rglob("*") if p.is_file()}
    # Prior execution code may legitimately be replaced; it is not a training dependency.
    case.old_code.write_text("# changed current execution code after the original failed execution\n")
    result = run(case)
    for name, payload in original.items():
        assert (case.models / name).read_bytes() == payload
        if name not in {"manifest.json", "declaration.json"}:
            assert (case.output / name).read_bytes() == payload
    old_manifest = json.loads(original["manifest.json"])
    assert result["manifest"]["started_at"] == old_manifest["started_at"]
    assert result["manifest"]["completed_at"] == old_manifest["completed_at"]
    assert (case.output / "reuse_provenance/original_training_declaration.json").read_bytes() == original["declaration.json"]
    assert (case.output / "declaration.json").read_bytes() == case.new.read_bytes()
    receipt = reuse.verify_reused_stability_models(case.output, case.new, case.audit)
    assert receipt["status"] == "passed" and receipt["training_reperformed"] is False
    assert receipt["bound_inputs"]["original_protocol"]["sha256"] == reuse._sha(case.old)


@pytest.mark.parametrize("field", ["training", "periods", "execution", "validity", "execution_rules",
    "source_snapshot_manifest_sha256", "historical_scope_manifest_sha256", "implementation_sha256"])
def test_any_nonexecution_protocol_change_requires_retraining(tmp_path, monkeypatch, field):
    case = package(tmp_path, monkeypatch)
    value = json.loads(case.new.read_text())
    value[field] = {"changed": True}
    write(case.new, value)
    with pytest.raises(ValueError, match="only declaration time"):
        run(case)
    assert not case.output.exists()


@pytest.mark.parametrize("relative", ["predictions.csv", "LGB_2026/model/model.txt", "LGB_2025/protocol.json"])
def test_mutated_original_model_prediction_or_fit_metadata_is_rejected(tmp_path, monkeypatch, relative):
    case = package(tmp_path, monkeypatch)
    path = case.models / relative
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="artifact bytes changed"):
        run(case)
    assert not case.output.exists()


def test_omitted_unlisted_file_and_native_rehashed_metadata_do_not_bypass_guards(tmp_path, monkeypatch):
    case = package(tmp_path, monkeypatch)
    extra = case.models / "unlisted-original.bin"
    extra.write_bytes(b"must not silently drop this artifact")
    with pytest.raises(ValueError, match="omits or invents"):
        run(case)
    extra.unlink()
    path = case.models / "LGB_2025/model/manifest.json"
    native = json.loads(path.read_text())
    native["metadata"]["declaration_sha256"] = reuse._sha(case.new)
    native.pop("manifest_sha256")
    native["manifest_sha256"] = native_sha(native)
    write(path, native)
    seal(case.models / "LGB_2025")
    manifest = json.loads((case.models / "manifest.json").read_text())
    manifest["files"] = {p.relative_to(case.models).as_posix(): reuse._sha(p)
        for p in case.models.rglob("*") if p.is_file() and p != case.models / "manifest.json"}
    write(case.models / "manifest.json", manifest)
    with pytest.raises(ValueError, match="original training declaration"):
        run(case)


def test_source_changes_and_existing_outputs_cannot_be_reused(tmp_path, monkeypatch):
    case = package(tmp_path, monkeypatch)
    case.feature.write_bytes(b"modified source buffer")
    with pytest.raises(ValueError, match="source bytes changed"):
        run(case)
    assert not case.output.exists()
    case.output.mkdir()
    with pytest.raises(ValueError, match="Preserve original models"):
        run(case)


@pytest.mark.parametrize("target", ["original_prediction", "original_protocol", "new_protocol", "copied_native", "source"])
def test_before_and_after_verifier_follows_originals_not_only_copied_package(tmp_path, monkeypatch, target):
    case = package(tmp_path, monkeypatch)
    run(case)
    path = {"original_prediction": case.models / "predictions.csv", "original_protocol": case.old,
        "new_protocol": case.new, "copied_native": case.output / "LGB_2026/model/model.txt", "source": case.feature}[target]
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        reuse.verify_reused_stability_models(case.output, case.new, case.audit)
