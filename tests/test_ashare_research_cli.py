"""New research commands dispatch explicitly without running data jobs in tests."""
import builtins
from pathlib import Path

from click.testing import CliRunner
import pytest

from stopat30m.research_cli import research_group


@pytest.mark.parametrize("command", ["ashare-diagnose", "ashare-optimize-review", "ashare-rolling-fit"])
def test_research_help_keeps_model_and_optimization_imports_lazy(monkeypatch, command):
    original = builtins.__import__
    blocked = {"stopat30m.ashare_diagnostics", "stopat30m.ashare_optimization", "stopat30m.ashare_rolling_research"}
    def imported(name, *args, **kwargs):
        if name in blocked:
            pytest.fail("Command help must not load research models or data")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imported)
    result = CliRunner().invoke(research_group, [command, "--help"])
    assert result.exit_code == 0, result.output
    assert "--output-dir" in result.output


def test_diagnostics_dispatch_uses_existing_artifact_and_training_origin(tmp_path, monkeypatch):
    from stopat30m import ashare_diagnostics
    calls = []
    def diagnose(*args):
        calls.append(args)
        return {"strategy_cost_attribution": {"lightgbm": {"net_profit_cny": "6281.25", "costs": {"total_friction_cny": "20040.24"}}}}
    monkeypatch.setattr(ashare_diagnostics, "run_diagnostics", diagnose)
    output = tmp_path / "new-diagnostics"
    result = CliRunner().invoke(research_group, ["ashare-diagnose", "--experiment-dir", str(tmp_path),
        "--training-origin", str(tmp_path), "--output-dir", str(output)])
    assert result.exit_code == 0, result.output
    assert calls == [(tmp_path, tmp_path, output)]
    assert "6281.25 CNY" in result.output and "not zero-cost" in result.output
    assert not output.exists()


@pytest.mark.parametrize("mode", ["execution", "ablation", "rolling"])
def test_review_options_dispatch_to_the_correct_api(tmp_path, monkeypatch, mode):
    from stopat30m import ashare_optimization
    calls = []
    report = {"cases": {"M0_E0": {"metrics": {"annual_return": .0363, "max_drawdown": -.1163,
        "incomplete_valuation_days": 0}}},
              "selection_status": "blocked_required_2023_2024_execution_data_and_historical_rules_not_verified"}
    def existing(*args, **kwargs):
        calls.append(("existing", args, kwargs))
        return report
    def rolling(*args, **kwargs):
        calls.append(("rolling", args, kwargs))
        return report
    monkeypatch.setattr(ashare_optimization, "run_existing_model_review", existing)
    monkeypatch.setattr(ashare_optimization, "run_rolling_model_review", rolling)
    spec = tmp_path / "spec.json"
    spec.write_text("{}")
    output = tmp_path / "new-review"
    extra = ["--cost-ablation"] if mode == "ablation" else ["--rolling-predictions-dir", str(tmp_path)] if mode == "rolling" else []
    result = CliRunner().invoke(research_group, ["ashare-optimize-review", "--baseline-dir", str(tmp_path),
        "--spec", str(spec), "--output-dir", str(output), *extra])
    assert result.exit_code == 0, result.output
    kind, args, kwargs = calls[0]
    assert kind == ("rolling" if mode == "rolling" else "existing")
    assert args == ((tmp_path, tmp_path, output) if mode == "rolling" else (tmp_path, output))
    assert kwargs["spec_path"] == spec and callable(kwargs["progress"])
    if mode != "rolling":
        assert kwargs["cost_ablation"] is (mode == "ablation")
    assert "3.63%" in result.output and "not fresh OOS" in result.output and "blocked_required" in result.output
    assert "incomplete valuation days 0" in result.output and "provisional" not in result.output


@pytest.mark.parametrize("incomplete", [5, None])
def test_review_marks_incomplete_or_unknown_valuation_as_provisional(tmp_path, monkeypatch, incomplete):
    from stopat30m import ashare_optimization
    metrics = {"annual_return": .25, "max_drawdown": -.10}
    if incomplete is not None:
        metrics["incomplete_valuation_days"] = incomplete
    report = {"cases": {"M1_E0": {"metrics": metrics}}, "selection_status": "blocked_required_validation"}
    monkeypatch.setattr(ashare_optimization, "run_rolling_model_review", lambda *args, **kwargs: report)
    result = CliRunner().invoke(research_group, ["ashare-optimize-review", "--baseline-dir", str(tmp_path),
        "--rolling-predictions-dir", str(tmp_path), "--output-dir", str(tmp_path/"review")])
    assert result.exit_code == 0, result.output
    assert f"incomplete valuation days {incomplete if incomplete is not None else 'unknown'}" in result.output
    assert "M1_E0: provisional" in result.output
    assert "incomplete valuation is not a verified performance comparison" in result.output
    assert "not fresh OOS" in result.output and "does not switch the active paper model" in result.output


def test_mutually_exclusive_reviews_fail_before_import_or_any_job(tmp_path, monkeypatch):
    original = builtins.__import__
    def imported(name, *args, **kwargs):
        if name == "stopat30m.ashare_optimization":
            pytest.fail("Invalid options must fail before loading the review implementation")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imported)
    result = CliRunner().invoke(research_group, ["ashare-optimize-review", "--baseline-dir", str(tmp_path),
        "--output-dir", str(tmp_path / "never-created"), "--cost-ablation", "--rolling-predictions-dir", str(tmp_path)])
    assert result.exit_code == 2 and "mutually exclusive" in result.output
    assert not (tmp_path / "never-created").exists()


def test_review_validation_failure_is_a_clear_click_error(tmp_path, monkeypatch):
    from stopat30m import ashare_optimization
    def fail(*args, **kwargs):
        raise ValueError("Frozen source checksum mismatch")
    monkeypatch.setattr(ashare_optimization, "run_existing_model_review", fail)
    result = CliRunner().invoke(research_group, ["ashare-optimize-review", "--baseline-dir", str(tmp_path),
        "--output-dir", str(tmp_path / "absent")])
    assert result.exit_code == 1 and "Error: Frozen source checksum mismatch" in result.output
    assert "Traceback" not in result.output


def test_rolling_fit_dispatch_keeps_original_snapshot_and_spec_arguments(tmp_path, monkeypatch):
    from stopat30m import ashare_rolling_research
    calls = []
    def build(*args, **kwargs):
        calls.append((args, kwargs))
        return {"manifest": {"folds": [2023, 2024, 2025, 2026], "prediction_rows": 551018}}
    monkeypatch.setattr(ashare_rolling_research, "build_research_predictions", build)
    spec = tmp_path / "spec.json"
    spec.write_text("{}")
    output = tmp_path / "new-fit"
    result = CliRunner().invoke(research_group, ["ashare-rolling-fit", "--snapshot", str(tmp_path),
        "--baseline-origin", str(tmp_path), "--spec", str(spec), "--output-dir", str(output)])
    assert result.exit_code == 0, result.output
    assert calls[0][0] == (tmp_path, tmp_path, output, spec)
    assert callable(calls[0][1]["progress"])
    assert "Annual folds: 4; prediction rows: 551018" in result.output
    assert "retrospective research" in result.output


def test_missing_input_directory_is_rejected_by_click(tmp_path):
    result = CliRunner().invoke(research_group, ["ashare-diagnose", "--experiment-dir", str(tmp_path / "missing"),
        "--training-origin", str(tmp_path), "--output-dir", str(tmp_path / "output")])
    assert result.exit_code == 2 and "does not exist" in result.output
