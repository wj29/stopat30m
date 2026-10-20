"""Guard the full baseline and preregistered research decision, not just NAV."""
from copy import deepcopy
import json

import pandas as pd
import pytest
from click.testing import CliRunner

from stopat30m.ashare_turnover_experiment import (
    PERIODS, _write_result, assert_full_baseline_reproduced, decide_comparison, register_commands,
)
from stopat30m.ashare_turnover_validation import FIXED_VARIANTS


@pytest.fixture
def baseline(tmp_path):
    records = {"trades": [{"instrument": "SH600000", "quantity": 100, "commission": "5.00"}],
        "orders": [{"status": "rejected", "reason": "limit"}],
        "curves": [{"equity": "100000.00"}], "plans": [{"exits": ["SH600000"]}],
        "valuation_audit": [], "score_coverage": []}
    result = {"period": "2025", "model": "LGB", "performance_valid": True,
              "metrics": {"net": "0"}, "stopped": None, "score_quality": {"mean": .1},
              "final_state": {"cash": "100000.00", "claims": []}, "records": records}
    (tmp_path / "summary.json").write_text(json.dumps({k: v for k, v in result.items()
        if k not in {"records", "final_state"}}))
    (tmp_path / "final_state.json").write_text(json.dumps(result["final_state"]))
    for name, rows in records.items():
        if name in {"plans", "valuation_audit", "score_coverage"}:
            (tmp_path / (name + ".json")).write_text(json.dumps(rows))
        else:
            pd.DataFrame(rows).to_csv(tmp_path / (name + ".csv"), index=False)
    return result, tmp_path


def test_complete_baseline_matches(baseline):
    result, path = baseline
    receipt = assert_full_baseline_reproduced(result, path)
    assert receipt["passed"] and receipt["record_types"] == sorted(result["records"])


@pytest.mark.parametrize("part", ["order", "trade", "plan", "final", "validity", "score", "missing"])
def test_same_nav_does_not_mask_changed_ledger(baseline, part):
    original, path = baseline
    result = deepcopy(original)
    if part == "order":
        result["records"]["orders"][0]["reason"] = "cash"
    elif part == "trade":
        result["records"]["trades"][0]["quantity"] = 200
    elif part == "plan":
        result["records"]["plans"][0]["exits"] = []
    elif part == "final":
        result["final_state"]["claims"] = [{"unpriced": True}]
    elif part == "validity":
        result["performance_valid"] = False
    elif part == "score":
        result["score_quality"]["mean"] = .2
    else:
        result["records"].pop("orders")
    with pytest.raises(ValueError, match="Baseline"):
        assert_full_baseline_reproduced(result, path)


def _paths():
    return [{"variant": v["key"], "period": p, "performance_valid": True,
        "metrics": {"total_return_lower_exact": "-0.1",
                    "max_drawdown_conservative_exact": "-0.25",
                    "one_way_turnover_average_nav": 38.0}}
        for v in FIXED_VARIANTS for p in PERIODS]


def _improve(rows, key, *, returns="0.05", drawdown="-0.2", turnover=20.0):
    for row in rows:
        if row["variant"] == key:
            row["metrics"].update(total_return_lower_exact=returns,
                max_drawdown_conservative_exact=drawdown, one_way_turnover_average_nav=turnover)


def test_profit_drawdown_and_turnover_all_required():
    rows = _paths()
    _improve(rows, "hold10_cap2", drawdown="-0.3")
    _improve(rows, "hold10_cap5", turnover=40.0)
    _improve(rows, "hold20_cap2", returns="-0.11")
    decision = decide_comparison(rows)
    assert decision["execution_improvement_variants"] == []
    assert decision["next_research"] == "separate_20_session_label_research_justified"
    assert decision["twenty_day_model_trained"] is False


def test_stable_positive_improvement_defers_new_model():
    rows = _paths()
    _improve(rows, "hold10_cap2")
    decision = decide_comparison(rows)
    assert decision["positive_stable_improvement_variants"] == ["hold10_cap2"]
    assert decision["next_research"] == "defer_20_session_model_test_five_day_signal_forward"
    assert decision["strategy_promoted"] is False


def test_positive_continuous_but_losing_one_year_does_not_pass():
    rows = _paths()
    _improve(rows, "hold10_cap2")
    next(r for r in rows if r["variant"] == "hold10_cap2" and r["period"] == "2026")["metrics"]["total_return_lower_exact"] = "-0.01"
    decision = decide_comparison(rows)
    assert decision["execution_improvement_variants"] == ["hold10_cap2"]
    assert decision["positive_stable_improvement_variants"] == []


def test_invalid_path_is_not_qualified_or_selected():
    rows = _paths()
    _improve(rows, "hold10_cap2")
    next(r for r in rows if r["variant"] == "hold10_cap2" and r["period"] == "2025")["performance_valid"] = False
    decision = decide_comparison(rows)
    assert decision["execution_improvement_variants"] == []
    assert decision["next_research"] == "resolve_invalid_paths"


@pytest.mark.parametrize("change", ["missing", "duplicate", "extra"])
def test_all_fifteen_paths_required(change):
    rows = _paths()
    if change == "missing":
        rows.pop()
    elif change == "duplicate":
        rows.append(rows[0])
    else:
        rows[-1]["variant"] = "hold30_cap1"
    with pytest.raises(ValueError, match="exactly once"):
        decide_comparison(rows)


def test_cli_exposes_no_training_option():
    import click
    @click.group()
    def cli():
        pass
    register_commands(cli)
    runner = CliRunner()
    assert set(cli.commands) == {"ashare-turnover-freeze", "ashare-turnover-validate"}
    result = runner.invoke(cli, ["ashare-turnover-validate", "--help"])
    assert result.exit_code == 0 and "--declaration" in result.output
    assert "--train" not in result.output


def test_nested_turnover_audits_preserve_types(baseline, tmp_path):
    result, _ = baseline
    result["records"].update(turnover_plans=[{"allowed_exits": ["SH600000"], "holding_age_sessions": {"SH600000": 10}}],
        holding_age=[{"holding_age_sessions": 10}], holding_origin_actions=[{"origin": "2025-01-02"}])
    output = tmp_path / "export"
    summary = _write_result(result, output, FIXED_VARIANTS[1])
    assert summary["minimum_commission_fill_fraction"] == 1
    for name in ("turnover_plans", "holding_age", "holding_origin_actions"):
        assert json.loads((output / (name + ".json")).read_text()) == result["records"][name]
        assert not (output / (name + ".csv")).exists()
