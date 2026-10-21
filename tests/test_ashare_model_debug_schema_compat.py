"""Only the original A6 proof field is adapted; frozen evidence stays immutable."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from stopat30m import ashare_model_debug_schema_compat as compat

base = compat.base


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


def setup(tmp_path, monkeypatch):
    static = {}
    for key in ("model_debug_training", "model_debug_validation", "model_debug_experiment"):
        path = tmp_path / (key + ".py"); path.write_text("# unchanged original source\n")
        static[key] = base.stability._proof(path)
    baseline = tmp_path / "A6"; baseline.mkdir()
    originals = {}
    for name in ("model_manifest", "protocol", "market_audit", "snapshot_manifest", "scope_manifest", "merger_actions"):
        path = write(tmp_path / "originals" / (name + ".json"), {"source": name})
        originals[name] = base.stability._proof(path)
    original_report = write(baseline / "report.json", {"kind": "a6_fixed_lgb_turnover_results", "bound_inputs": {
        "original_model_manifest": originals["model_manifest"],
        **{key: value for key, value in originals.items() if key != "model_manifest"}}})
    write(baseline / "manifest.json", {"kind": "a6_fixed_lgb_turnover_results", "files": {"report.json": base.stability.sha(original_report)}})
    protocol = {"kind": base.KIND, "active_paper_directory": str(tmp_path / "active_paper"),
        "design": {"execution": {"top_k": 20, "keep_rank": 40}},
        "merger_actions": originals["merger_actions"],
        "source_families": {"stability": {key: originals[key] for key in ("model_manifest", "protocol", "market_audit")}},
        "bound_inputs": {**static, "stability:snapshot_manifest": originals["snapshot_manifest"],
            "stability:scope_manifest": originals["scope_manifest"]}}
    declaration = write(tmp_path / "training.json", protocol)
    selection = write(tmp_path / "selection.json", {"selected_candidate": "regularized300", "primary_qualified": False})
    models = {}
    for phase, count in (("selection", 3), ("confirmation", 1), ("retrospective", 2)):
        path = write(tmp_path / (phase + "models") / "manifest.json", {"kind": "synthetic_model", "phase": phase,
            "models": list(base.CANDIDATES) if phase == "selection" else ["regularized300"],
            "native_fit_count": count, "inputs": {"selection_decision": base.stability._proof(selection)}})
        models[phase] = path.parent
    chosen = {"selected_candidate": "regularized300", "primary_qualified": False,
        "selection_model_manifest": base.stability._proof(models["selection"] / "manifest.json")}
    log = tmp_path / "original_failure.log"; log.write_text("Original pipeline failed before any retrospective ledger\n" + compat.REASON + "\n")
    monkeypatch.setattr(base, "load_debug_declaration", lambda _: protocol)
    monkeypatch.setattr(base, "_verify_selection", lambda *args: deepcopy(chosen))
    monkeypatch.setattr(base, "implementation_proofs", lambda: {key: base.stability._proof(v["path"]) for key, v in static.items()})
    def verify_model(directory, declaration_path, phase):
        root = Path(directory); manifest = base._read(root / "manifest.json")
        assert manifest["phase"] == phase
        return manifest, {"new_model:" + phase + ":manifest": base.stability._proof(root / "manifest.json")}
    monkeypatch.setattr(base, "_new_model_proofs", verify_model)
    args = {"declaration_path": declaration, "models_directory": models["retrospective"],
        "output": tmp_path / "retrospective_execution.json", "selection_path": selection,
        "confirmation_models_directory": models["confirmation"], "failure_log": log,
        "erratum_output": tmp_path / "schema_erratum.json", "ledger_output_directory": tmp_path / "new_ledger",
        "baseline_directory": baseline}
    return args, protocol, models, originals, static


def reseal_baseline(args):
    root = args["baseline_directory"]
    write(root / "manifest.json", {"kind": "a6_fixed_lgb_turnover_results", "files": {"report.json": base.stability.sha(root / "report.json")}})


def test_only_original_schema_key_is_consumed_and_all_evidence_stays_identical(tmp_path, monkeypatch):
    args, protocol, models, _, _ = setup(tmp_path, monkeypatch)
    before = {str(p): base.stability.sha(p) for p in tmp_path.rglob("*") if p.is_file()}
    result = compat.freeze_compatible_retrospective_declaration(**args)
    assert result["kind"] == base.EXECUTION_KIND and result["phase"] == "retrospective"
    assert result["selected_candidate"] == "regularized300" and result["expected_new_ledger_count"] == 6
    assert result["execution"] == protocol["design"]["execution"]
    assert result["selection_models_directory"] is None
    assert result["schema_compatibility"]["original_runner_and_three_baseline_gate_unchanged"] is True
    assert all(base.stability.sha(p) == digest for p, digest in before.items())
    assert not args["ledger_output_directory"].exists()
    erratum = base._read(args["erratum_output"])
    assert erratum["actual_original_field"] == "original_model_manifest"
    assert erratum["frozen_freezer_expected_nonexistent_field"] == "predictions_manifest"
    assert erratum["native_fits_preserved"] == 6
    assert erratum["retrospective_returns_before_correction"] == erratum["fits_predictions_or_ledger_runs_here"] == 0
    assert erratum["old_evidence_modified_or_aliased"] is False
    assert erratum["training_or_model_or_selection_or_fee_or_strategy_changes"] is False
    assert set(erratum["six_model_packages"]) == set(base.PHASES)
    assert len(erratum["frozen_three_sources"]) == 3
    assert base.stability._proof(args["erratum_output"]) == result["bound_inputs"]["retrospective_schema_erratum"]
    archive = Path(result["bound_inputs"]["archived_schema_compat_helper"]["path"])
    assert archive.read_bytes() == Path(compat.__file__).read_bytes()


@pytest.mark.parametrize("key", ["original_model_manifest", "protocol", "market_audit", "snapshot_manifest", "scope_manifest", "merger_actions"])
def test_real_source_identity_is_never_waived_or_rebound(tmp_path, monkeypatch, key):
    args, *_ = setup(tmp_path, monkeypatch)
    path = args["baseline_directory"] / "report.json"; report = base._read(path)
    report["bound_inputs"][key] = {"path": str(path), "sha256": "0" * 64}
    write(path, report); reseal_baseline(args)
    with pytest.raises(ValueError, match="real field: " + key):
        compat.freeze_compatible_retrospective_declaration(**args)
    assert not args["output"].exists() and not args["erratum_output"].exists()


def test_missing_actual_key_is_not_replaced_by_an_invented_alias(tmp_path, monkeypatch):
    args, *_ = setup(tmp_path, monkeypatch)
    path = args["baseline_directory"] / "report.json"; report = base._read(path)
    report["bound_inputs"]["predictions_manifest"] = report["bound_inputs"].pop("original_model_manifest")
    write(path, report); reseal_baseline(args)
    with pytest.raises(ValueError, match="restricted to the original A6 schema"):
        compat.freeze_compatible_retrospective_declaration(**args)


@pytest.mark.parametrize("phase", ["selection", "confirmation", "retrospective"])
def test_no_additional_or_missing_native_fit_budget(tmp_path, monkeypatch, phase):
    args, _, models, *_ = setup(tmp_path, monkeypatch)
    path = models[phase] / "manifest.json"; manifest = base._read(path); manifest["native_fit_count"] += 1; write(path, manifest)
    # Selection model identity itself changes, so its earlier immutable proof
    # also has to fail; later phases specifically reach the budget guard.
    with pytest.raises(ValueError): compat.freeze_compatible_retrospective_declaration(**args)


@pytest.mark.parametrize("phase", ["confirmation", "retrospective"])
@pytest.mark.parametrize("fault", ["candidate", "decision"])
def test_later_model_or_decision_cannot_change(tmp_path, monkeypatch, phase, fault):
    args, _, models, *_ = setup(tmp_path, monkeypatch)
    path = models[phase] / "manifest.json"; manifest = base._read(path)
    if fault == "candidate": manifest["models"] = ["capacity300"]
    else: manifest["inputs"]["selection_decision"] = {"path": str(path), "sha256": "0" * 64}
    write(path, manifest)
    with pytest.raises(ValueError, match="exact original 2023 decision"):
        compat.freeze_compatible_retrospective_declaration(**args)


@pytest.mark.parametrize("key", ["model_debug_training", "model_debug_validation", "model_debug_experiment"])
def test_original_three_sources_cannot_be_changed(tmp_path, monkeypatch, key):
    args, _, _, _, static = setup(tmp_path, monkeypatch)
    Path(static[key]["path"]).write_text("# modified frozen source")
    with pytest.raises(ValueError, match="frozen A14"):
        compat.freeze_compatible_retrospective_declaration(**args)


def test_requires_exact_recorded_preledger_failure(tmp_path, monkeypatch):
    args, *_ = setup(tmp_path, monkeypatch)
    args["failure_log"].write_text("an unrelated failure")
    with pytest.raises(ValueError, match="original pre-ledger schema failure"):
        compat.freeze_compatible_retrospective_declaration(**args)


def test_existing_retrospective_returns_prevent_new_pre_result_seal(tmp_path, monkeypatch):
    args, *_ = setup(tmp_path, monkeypatch)
    args["ledger_output_directory"].mkdir()
    (args["ledger_output_directory"] / "curves.csv").write_text("existing evidence")
    with pytest.raises(ValueError): compat.freeze_compatible_retrospective_declaration(**args)
    assert not args["output"].exists() and not args["erratum_output"].exists()


def test_concurrent_ledger_appearance_is_not_hidden(tmp_path, monkeypatch):
    args, *_ = setup(tmp_path, monkeypatch)
    original = base.stability._assert_proofs
    calls = []
    def verify(proofs):
        original(proofs)
        calls.append(len(proofs))
        if "schema_compat_helper" in proofs and "retrospective_schema_erratum" not in proofs:
            args["ledger_output_directory"].mkdir(exist_ok=True)
    monkeypatch.setattr(base.stability, "_assert_proofs", verify)
    with pytest.raises(ValueError, match="already exists"):
        compat.freeze_compatible_retrospective_declaration(**args)
    assert not args["output"].exists()


def test_erratum_and_execution_outputs_are_not_overwritten(tmp_path, monkeypatch):
    args, *_ = setup(tmp_path, monkeypatch)
    args["erratum_output"].write_text("original erratum")
    with pytest.raises(ValueError): compat.freeze_compatible_retrospective_declaration(**args)
    assert args["erratum_output"].read_text() == "original erratum"
