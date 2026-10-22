"""Reporting must carry actual NAV, never compound reset-year backtests."""
import importlib.util
from pathlib import Path
import sys
import subprocess

import numpy as np
import pandas as pd
import pytest


SCRIPTS = Path(__file__).parents[1] / "scripts"
SPEC = importlib.util.spec_from_file_location("a16_baseline_analysis", SCRIPTS / "analyze_model_baseline_comparison.py")
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def account(*, valid=True):
    return {"summary":{"period":"continuous","candidate":"original","stopped":None,
        "metrics":{"total_return_lower_exact":".21","max_drawdown_conservative_exact":"-.03",
            "one_way_turnover_average_nav":2.,"mean_invested_fraction":.9,
            "total_fee_cny":"100.00","slippage_cost_cny":"50.00","incomplete_valuation_days":0,
            "accounting_invalid_days":0 if valid else 1,"positive_month_fraction":1.,
            "rolling_63_sessions":{},"rolling_126_sessions":{}}},
        "dates":pd.Series(pd.to_datetime(["2025-12-30","2025-12-31","2026-01-05","2026-04-01"])),
        "nav":np.asarray([1.05,1.10,1.11,1.21]),"valid":valid,"costs":{}}


def test_actual_prior_close_anchors_all_year_quarter_and_month_segments():
    row=module.summarize_account(account(),"stability")
    segments=row["calendar_segments"]
    assert segments["years"]["2025"]["net_return"]==pytest.approx(.1)
    assert segments["years"]["2026"]["net_return"]==pytest.approx(.1)
    assert segments["quarters"]["2026Q1"]["net_return"]==pytest.approx(1.11/1.10-1)
    assert segments["quarters"]["2026Q2"]["net_return"]==pytest.approx(1.21/1.11-1)
    assert segments["months"]["2026-04"]["net_return"]==pytest.approx(1.21/1.11-1)
    assert row["net_return"]==.21
    assert row["fees_plus_embedded_slippage_cny"]==150.


def test_whole_invalid_account_segments_stay_diagnostic_even_if_individual_bins_look_good():
    row=module.summarize_account(account(valid=False),"stability")
    assert row["financially_valid"] is False
    for bins in row["calendar_segments"].values():
        assert all(v["diagnostic_only"] and not v["whole_path_financially_valid"] for v in bins.values())


def test_overlapping_or_existing_analysis_output_cannot_change_sealed_experiment(tmp_path):
    directory=tmp_path/"sealed";directory.mkdir()
    for output in (directory,directory/"nested"):
        with pytest.raises(ValueError):module.analyze(directory,output)
    existing=tmp_path/"existing";existing.mkdir()
    with pytest.raises(ValueError):module.analyze(directory,existing)


@pytest.mark.parametrize("protected",["forward","models"])
def test_even_new_analysis_subdirectories_inside_accounts_or_models_are_forbidden(tmp_path,monkeypatch,protected):
    from stopat30m import ashare_stability_validation as stability
    monkeypatch.setattr(stability,"guard_qlib_output_path",lambda path:path)
    output=tmp_path/protected/"new-analysis"
    with pytest.raises(ValueError,match="paper account or model package"):
        module.analyze(tmp_path/"sealed",output)
    assert not output.exists()


def test_module_cli_can_import_both_project_and_unchanged_reconciler():
    result=subprocess.run([sys.executable,"-m","scripts.analyze_model_baseline_comparison","--help"],
        cwd=SCRIPTS.parent,capture_output=True,text=True)
    assert result.returncode==0, result.stderr
    assert "--directory" in result.stdout and "--output" in result.stdout
