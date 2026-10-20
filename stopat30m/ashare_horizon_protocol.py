"""Immutable design and training-source declaration for a matched label trial."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path

from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
from stopat30m.data.qlib_snapshot import guard_qlib_output_path
from stopat30m.model.trainer import FixedQlibLGBSpec


KIND = "a8_shared_panel_horizon_protocol"
MODELS = ("H5", "H20")
LABELS = {"H5": "Ref($open,-6)/Ref($open,-1)-1", "H20": "Ref($open,-21)/Ref($open,-1)-1"}
EXECUTION = {**stability.EXECUTION, "rebalance_sessions": 20}
PERIODS = {"2025": ["2025-01-01", "2025-12-31"],
    "2026": ["2026-01-01", "2026-10-19"], "continuous": ["2025-01-01", "2026-10-19"]}
TRAINING = {"execution_years": [2025, 2026], "evaluation_end": "2026-10-19",
    "lookback_calendar_years": 5, "common_purge_future_sessions": 21,
    "entry_delay_sessions": 1, "labels": LABELS, "common_endpoint_offsets": [1, 6, 21],
    "membership_filter": "signal_date_only_no_future_membership_survival_condition",
    "shared_feature_processors": True, "common_training_panel": True,
    "fixed_lightgbm_spec": asdict(FixedQlibLGBSpec()),
    "hyperparameter_search": False, "early_stopping": False}
VALIDITY = {**stability.VALIDITY,
    "retraining": "annual_previous_five_calendar_years_shared_twenty_one_session_purge",
    "score_quality": "arm_specific_observed_open_t_plus_1_to_t_plus_horizon_plus_1",
    "label_comparison": "new_matched_H5_control_and_H20_experiment_not_original_H5_reuse",
    "minimum_hold_or_replacement_caps_added": False,
    "economic_rights_proxy_can_override_invalidity": False,
    "future_paper_or_broker_promotion": False}


def _read(path):
    return json.loads(Path(path).read_bytes())


def _assert_design(design):
    if (design.get("kind") != "a8_shared_panel_label_horizon_design"
            or design.get("schema_version") != 1 or design.get("models") != list(MODELS)
            or design.get("labels") != LABELS or design.get("training") != TRAINING
            or design.get("execution") != EXECUTION or design.get("periods") != PERIODS
            or design.get("fixed_fit_budget") != 4 or design.get("fixed_ledger_budget") != 6
            or design.get("minimum_hold_and_replacement_caps_from_A6_added") is not False
            or design.get("all_new_fits_and_returns_computed_before_declaration") is not False):
        raise ValueError("The unchanged preimplementation H5/H20 shared-panel design is required")
    expected_gate = {"all_six_paths_accounting_valid": True,
        "higher_continuous_net_return_than_H5": True,
        "continuous_max_drawdown_no_worse_than_H5": True,
        "continuous_turnover_no_higher_than_H5": True,
        "H20_positive_net_return_all_three_periods": True}
    if design.get("minimum_evidence_for_H20_improvement") != expected_gate:
        raise ValueError("Do not change the research decision gate after declaration")


def freeze_horizon_protocol(design_freeze, output):
    """Bind the source, paired fit policy and training code before four fits.

    A later execution declaration binds the completed model package and ledger
    implementation before returns. Execution development cannot silently alter
    the fitted training package or force an unnecessary refit.
    """
    from stopat30m import ashare_horizon_research as research
    output = guard_qlib_output_path(output)
    if output.exists():
        raise ValueError("Preserve every previous training declaration")
    frozen = _read(design_freeze)
    if frozen.get("kind") != "a8_preimplementation_label_horizon_design_freeze":
        raise ValueError("A prior label-horizon design freeze is required")
    _assert_design(frozen["design"])
    if _read(frozen["bound_design"]["path"]) != frozen["design"]:
        raise ValueError("The original design no longer matches its freeze")
    bound = {key: frozen[key] for key in ("bound_design", "source_bundle",
        "source_snapshot_manifest", "historical_scope_manifest", "market_audit", "stock_basic",
        "original_protocol", "original_model_manifest", "original_comparison_report",
        "original_comparison_manifest", "merger_actions", "old_prediction_bytes_unchanged")}
    bound.update(design_freeze=stability._proof(design_freeze),
        protocol_implementation=stability._proof(__file__))
    stability._assert_proofs(bound)
    original_package = Path(bound["original_model_manifest"]["path"]).parent
    old_model_receipt = verify_reused_stability_models(original_package,
        bound["original_protocol"]["path"], bound["market_audit"]["path"])
    bundle = _read(bound["source_bundle"]["path"])
    if (stability._proof(Path(bundle["snapshot_path"])/"manifest.json") != bound["source_snapshot_manifest"]
            or stability._proof(Path(bundle["scope_path"])/"manifest.json") != bound["historical_scope_manifest"]):
        raise ValueError("Original source-bundle identities changed")
    payload = {"kind": KIND, "schema_version": 1,
        "declared_at": datetime.now(timezone.utc).isoformat(), "models": list(MODELS),
        "source_snapshot_manifest_sha256": bound["source_snapshot_manifest"]["sha256"],
        "historical_scope_manifest_sha256": bound["historical_scope_manifest"]["sha256"],
        "implementation_sha256": research.implementation_sha256(),
        "training": TRAINING, "execution": EXECUTION, "validity": VALIDITY,
        "periods": PERIODS, "design": frozen["design"], "bound_inputs": bound,
        "execution_clock": "first_actual_session_of_each_account_anchors_phase_continuous_has_no_year_reset",
        "old_models_verified_and_preserved": old_model_receipt,
        "new_model_package_must_be_complete_before_execution_declaration": True,
        "active_account_change_allowed": False}
    stability._assert_proofs(bound)
    output.parent.mkdir(parents=True, exist_ok=True)
    stability._save(output, payload)
    return payload


def load_horizon_protocol(path):
    """Reject any altered parameters, source generation or fit implementation."""
    from stopat30m import ashare_horizon_research as research
    protocol = _read(path)
    if (protocol.get("kind") != KIND or protocol.get("schema_version") != 1
            or protocol.get("models") != list(MODELS) or protocol.get("training") != TRAINING
            or protocol.get("execution") != EXECUTION or protocol.get("validity") != VALIDITY
            or protocol.get("periods") != PERIODS
            or protocol.get("implementation_sha256") != research.implementation_sha256()
            or protocol.get("active_account_change_allowed") is not False):
        raise ValueError("The fixed shared-panel label trial changed after declaration")
    _assert_design(protocol["design"])
    bound = protocol["bound_inputs"]
    stability._assert_proofs(bound)
    if (protocol["source_snapshot_manifest_sha256"] != bound["source_snapshot_manifest"]["sha256"]
            or protocol["historical_scope_manifest_sha256"] != bound["historical_scope_manifest"]["sha256"]
            or bound["protocol_implementation"] != stability._proof(__file__)):
        raise ValueError("Horizon training source or protocol implementation changed")
    return protocol
