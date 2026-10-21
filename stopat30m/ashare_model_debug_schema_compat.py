"""Transparent A14 retrospective seal for the original A6 proof-key schema.

The immutable A6 report calls its model identity ``original_model_manifest``.
The frozen A14 generic freezer expects ``predictions_manifest`` and therefore
stops before sealing any retrospective execution. This compatibility builder
reads the original field directly, preserves every original byte, and binds
this correction as a separate engineering erratum. It never trains, predicts,
replays a ledger, aliases evidence, or changes the frozen selection.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from stopat30m import ashare_model_debug_experiment as base


KIND = "a14_retrospective_original_A6_schema_erratum"
REASON = "Old stability baseline source identity differs: model_manifest"


def freeze_compatible_retrospective_declaration(declaration_path, models_directory, output, *,
        selection_path, confirmation_models_directory, failure_log, erratum_output,
        ledger_output_directory, baseline_directory=None):
    """Seal the same six paths and six models using A6's actual proof field.

    All original source and model guards run unmodified. The original runner
    subsequently reproduces its three complete A6 accounts before permitting
    any new retrospective candidate return.
    """
    protocol = base.load_debug_declaration(declaration_path)
    active = protocol["active_paper_directory"]
    output = base._isolated_output(output, active)
    erratum_output = base._isolated_output(erratum_output, active)
    ledger_output = base._isolated_output(ledger_output_directory, active)
    archive = base._isolated_output(erratum_output.with_suffix(".source.py"), active)
    if len({output, erratum_output, ledger_output, archive}) != 4:
        raise ValueError("Execution, erratum, archive and uncreated ledger outputs must be distinct")
    log_path = Path(failure_log).resolve()
    log_proof = base.stability._proof(log_path)
    if REASON not in log_path.read_text():
        raise ValueError("The unchanged original pre-ledger schema failure log is required")
    selected = base._verify_selection(selection_path, declaration_path)
    decision_proof = base.stability._proof(selection_path)
    selection_models = Path(selected["selection_model_manifest"]["path"]).parent
    directories = {"selection": selection_models,
        "confirmation": Path(confirmation_models_directory).resolve(),
        "retrospective": Path(models_directory).resolve()}
    model_proofs, manifests, manifest_proofs = {}, {}, {}
    for phase, directory in directories.items():
        manifest, proofs = base._new_model_proofs(directory, declaration_path, phase)
        manifests[phase] = manifest
        manifest_proofs[phase] = base.stability._proof(directory / "manifest.json")
        model_proofs.update(proofs)
    if manifest_proofs["selection"] != selected["selection_model_manifest"]:
        raise ValueError("The original 2023 selected model package must be preserved")
    for phase in ("confirmation", "retrospective"):
        if (manifests[phase].get("models") != [selected["selected_candidate"]]
                or manifests[phase].get("inputs", {}).get("selection_decision") != decision_proof):
            raise ValueError("Later models must preserve the exact original 2023 decision")
    if [manifests[phase].get("native_fit_count") for phase in base.PHASES] != [3, 1, 2]:
        raise ValueError("The unchanged three/one/two model packages are required; no new fits are allowed")
    baseline = Path(baseline_directory or base.DEFAULT_BASELINE).resolve()
    original_proofs = base._baseline_proofs(baseline)
    report = base._read(baseline / "report.json")
    original = report.get("bound_inputs", {})
    family = protocol["source_families"]["stability"]
    # Consume the real old schema. Never add an alias to, normalize, rebind, or
    # overwrite the original report, its manifest, or any source evidence.
    actual_source_keys = {"protocol": family["protocol"], "market_audit": family["market_audit"],
        "original_model_manifest": family["model_manifest"],
        "snapshot_manifest": protocol["bound_inputs"]["stability:snapshot_manifest"],
        "scope_manifest": protocol["bound_inputs"]["stability:scope_manifest"],
        "merger_actions": protocol["merger_actions"]}
    if "predictions_manifest" in original:
        raise ValueError("This compatibility correction is restricted to the original A6 schema")
    for key, expected in actual_source_keys.items():
        if original.get(key) != expected:
            raise ValueError("Original A6 source identity differs under its real field: " + key)
    base.stability._assert_proofs(actual_source_keys)
    implementation = base.implementation_proofs()
    frozen_three = {key: implementation[key] for key in
        ("model_debug_training", "model_debug_validation", "model_debug_experiment")}
    if any(protocol["bound_inputs"].get(key) != value for key, value in frozen_three.items()):
        raise ValueError("A frozen A14 training, validation or experiment source changed")
    source_proof = base.stability._proof(__file__)
    bound = {**protocol["bound_inputs"], **implementation, **model_proofs, **original_proofs,
        **base._collect_proofs(selected, "selection"),
        "training_declaration": base.stability._proof(declaration_path),
        "new_model_manifest": manifest_proofs["retrospective"],
        "selection_decision": decision_proof,
        "schema_compat_helper": source_proof,
        "original_A6_report_for_schema_correction": base.stability._proof(baseline / "report.json"),
        "original_failed_preledger_log": log_proof}
    base.stability._assert_proofs(bound)
    # The old failed attempt never created this directory. Recheck after all
    # potentially slow source verification and before writing the new seal.
    if ledger_output.exists():
        raise ValueError("Retrospective ledger output already exists; no pre-result seal is possible")
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_bytes(Path(__file__).read_bytes())
    bound["archived_schema_compat_helper"] = base.stability._proof(archive)
    erratum = {"kind": KIND, "schema_version": 1, "declared_at": datetime.now(timezone.utc).isoformat(),
        "correction": "read_original_A6_bound_inputs.original_model_manifest_directly",
        "frozen_freezer_expected_nonexistent_field": "predictions_manifest",
        "actual_original_field": "original_model_manifest", "original_field_proof": family["model_manifest"],
        "reason": REASON, "failed_before_any_retrospective_ledger": True,
        "original_failure_log": log_proof, "original_A6_report": bound["original_A6_report_for_schema_correction"],
        "original_A6_manifest": base.stability._proof(baseline / "manifest.json"),
        "training_declaration": bound["training_declaration"], "frozen_three_sources": frozen_three,
        "six_model_packages": manifest_proofs, "native_fits_preserved": 6,
        "frozen_selection_decision": decision_proof, "selected_candidate": selected["selected_candidate"],
        "compat_helper": source_proof, "archived_compat_helper": bound["archived_schema_compat_helper"],
        "ledger_output_not_yet_created": str(ledger_output), "retrospective_returns_before_correction": 0,
        "training_or_model_or_selection_or_fee_or_strategy_changes": False,
        "old_evidence_modified_or_aliased": False, "fits_predictions_or_ledger_runs_here": 0,
        "three_full_original_A6_reproductions_still_required_before_candidate_ledgers": True}
    erratum_output.parent.mkdir(parents=True, exist_ok=True)
    base.stability._save(erratum_output, erratum)
    bound["retrospective_schema_erratum"] = base.stability._proof(erratum_output)
    base.stability._assert_proofs(bound)
    if ledger_output.exists():
        raise ValueError("Retrospective returns appeared before the execution declaration was sealed")
    payload = {"kind": base.EXECUTION_KIND, "schema_version": 1,
        "declared_at": datetime.now(timezone.utc).isoformat(), "phase": "retrospective",
        "training_declaration": bound["training_declaration"], "bound_inputs": bound,
        "models_directory": str(directories["retrospective"]), "selection_models_directory": None,
        "baseline_directory": str(baseline), "selected_candidate": selected["selected_candidate"],
        "expected_new_ledger_count": 6, "execution": protocol["design"]["execution"],
        "active_paper_changed": False, "broker_order_sent": False,
        "models_verified_before_any_stage_return": True,
        "schema_compatibility": {"kind": KIND, "erratum": bound["retrospective_schema_erratum"],
            "source_schema_only": True, "original_runner_and_three_baseline_gate_unchanged": True}}
    output.parent.mkdir(parents=True, exist_ok=True)
    base.stability._save(output, payload)
    return payload


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("declaration_path", "models_directory", "output", "selection_path", "confirmation_models_directory",
                "failure_log", "erratum_output", "ledger_output_directory"):
        parser.add_argument("--" + key.replace("_", "-"), required=True, type=Path)
    parser.add_argument("--baseline-directory", type=Path)
    result = freeze_compatible_retrospective_declaration(**vars(parser.parse_args(argv)))
    print(result["kind"], result["phase"], result["schema_compatibility"]["erratum"]["sha256"])


if __name__ == "__main__":
    main()
