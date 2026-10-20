from dataclasses import asdict
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m.ashare_stability_research import (
    LABEL, MARKET_CHECKS, FixedQlibLGBSpec, _load_protocol, _sha,
    union_fold_plan, validate_market_audit,
)


def test_stability_folds_keep_prior_five_years_and_purged_labels():
    calendar = pd.bdate_range("2019-01-01", "2026-10-19")
    folds = union_fold_plan(calendar, [2025, 2026], evaluation_end="2026-10-19")
    assert [row["train_start"] for row in folds] == ["2020-01-01", "2021-01-01"]
    for row in folds:
        cutoff = calendar.get_loc(row["data_cutoff"])
        signal = calendar.get_loc(row["train_signal_end"])
        assert signal + 6 == cutoff
        assert row["signal_start"] == row["data_cutoff"]
        assert int(row["first_execution_session"][:4]) == row["execution_year"]
    for years in ([2023, 2024], [2023, 2024, 2025, 2026], [2026]):
        with pytest.raises(ValueError):
            union_fold_plan(calendar, years, evaluation_end="2026-10-19")


def test_primary_or_unfunded_tax_audit_cannot_authorize_stability_fit():
    audit = {"kind": "a5_stability_market_audit", "status": "passed", "source_preflight_only": False,
             "bound_inputs": {"distribution_tax_contracts": {"path": "bound by source checks"}},
             "source_snapshot_manifest_sha256": "source", "historical_scope_manifest_sha256": "scope",
             "stock_basic_sha256": "basic", "start": "2025-01-02", "end": "2026-10-19",
             "checks": dict.fromkeys(MARKET_CHECKS, True)}
    options = dict(source_sha="source", scope_sha="scope", stock_basic_sha="basic",
                   coverage_start="2025-01-02", coverage_end="2026-10-19")
    validate_market_audit(audit, **options)
    audit["source_preflight_only"] = True
    with pytest.raises(ValueError):
        validate_market_audit(audit, **options)
    audit["source_preflight_only"] = False
    contracts = audit["bound_inputs"].pop("distribution_tax_contracts")
    with pytest.raises(ValueError):
        validate_market_audit(audit, **options)
    audit["bound_inputs"]["distribution_tax_contracts"] = contracts
    audit["checks"]["distribution_tax_accounting"] = False
    with pytest.raises(ValueError):
        validate_market_audit(audit, **options)
    audit["checks"]["distribution_tax_accounting"] = True
    audit["kind"] = "a5_union_market_audit"
    with pytest.raises(ValueError):
        validate_market_audit(audit, **options)


def test_stability_protocol_rejects_primary_years_or_other_model_parameters(tmp_path):
    (tmp_path / "manifest.json").write_text("scope")
    source = SimpleNamespace(manifest_sha256="snapshot")
    value = {"kind": "a5_union_stability_fixed_research_protocol",
             "source_snapshot_manifest_sha256": "snapshot",
             "historical_scope_manifest_sha256": _sha(tmp_path / "manifest.json"),
             "training": {"execution_years": [2025, 2026], "lookback_calendar_years": 5,
                          "purge_future_sessions": 6, "hyperparameter_search": False,
                          "early_stopping": False, "label": LABEL, "evaluation_end": "2026-10-19",
                          "fixed_lightgbm_spec": asdict(FixedQlibLGBSpec()),
                          "membership_filter": "signal_date_only_no_future_membership_survival_condition"}}
    path = tmp_path / "protocol.json"
    path.write_text(json.dumps(value))
    assert _load_protocol(path, source, tmp_path)[1]["execution_years"] == [2025, 2026]
    value["training"]["execution_years"] = [2023, 2024, 2025, 2026]
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        _load_protocol(path, source, tmp_path)
