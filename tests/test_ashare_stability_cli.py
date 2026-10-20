import json

from click.testing import CliRunner
import pytest

from stopat30m.research_cli import research_group


def inputs(tmp_path):
    paths = {}
    for name in ("snapshot", "scope-dir", "predictions-dir"):
        path = tmp_path / name
        path.mkdir()
        paths[name] = path
    for name in ("protocol", "market-audit", "merger-actions", "rule-original-evidence"):
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps({"stock_basic_path": str(tmp_path / "basic.csv")}))
        paths[name] = path
    return paths


def arguments(paths, *, validate=False):
    names = ["snapshot", "scope-dir", "protocol", "market-audit"]
    if validate:
        names.extend(["predictions-dir", "merger-actions"])
    return [item for name in names for item in ("--" + name, str(paths[name]))]


def test_stability_commands_are_registered_without_starting_research():
    runner = CliRunner()
    for command in ("ashare-stability-freeze", "ashare-stability-train", "ashare-stability-validate"):
        result = runner.invoke(research_group, [command, "--help"])
        assert result.exit_code == 0
        assert "--snapshot" in result.output and "--scope-dir" in result.output
    result = runner.invoke(research_group, ["ashare-stability-train"])
    assert result.exit_code == 2 and "Missing option" in result.output


def test_freeze_routes_fixed_years_and_optional_original_rule_receipt(tmp_path, monkeypatch):
    from stopat30m import ashare_stability_validation as validation
    paths, received = inputs(tmp_path), {}
    output = tmp_path / "declaration.json"

    def freeze(path, snapshot, scope, **kwargs):
        received.update(path=path, snapshot=snapshot, scope=scope, **kwargs)
        return {"declared_at": "synthetic-route-test"}

    monkeypatch.setattr(validation, "freeze_stability_protocol", freeze)
    result = CliRunner().invoke(research_group, ["ashare-stability-freeze",
        "--snapshot", str(paths["snapshot"]), "--scope-dir", str(paths["scope-dir"]),
        "--evaluation-end", "2026-10-19", "--rule-original-evidence", str(paths["rule-original-evidence"]),
        "--output", str(output)])
    assert result.exit_code == 0, result.output
    assert received["path"] == output and received["execution_years"] == (2025, 2026)
    assert received["evaluation_end"] == "2026-10-19"
    assert received["rule_original_evidence_path"] == paths["rule-original-evidence"]


def test_train_uses_secondary_api_and_preserves_backend_gate_failure(tmp_path, monkeypatch):
    from stopat30m import ashare_stability_research as research
    paths, received = inputs(tmp_path), {}

    def train(*args, **kwargs):
        received.update(args=args, kwargs=kwargs)
        raise ValueError("Complete verified13-check source evidence required")

    monkeypatch.setattr(research, "build_union_predictions", train)
    output = tmp_path / "models"
    result = CliRunner().invoke(research_group, ["ashare-stability-train",
        *arguments(paths), "--output-dir", str(output)])
    assert result.exit_code == 1 and "13-check source evidence required" in result.output
    assert received["args"] == (paths["snapshot"], paths["scope-dir"], str(tmp_path / "basic.csv"),
                                 paths["protocol"], paths["market-audit"], output)
    assert callable(received["kwargs"]["progress"]) and not output.exists()


@pytest.mark.parametrize("all_valid", [False, True])
def test_invalid_validation_returns_nonzero_keeps_report_and_avoids_return_summary(tmp_path, monkeypatch, all_valid):
    from stopat30m import ashare_stability_validation as validation
    paths, received = inputs(tmp_path), {}
    output = tmp_path / "comparison"

    def validate(*args, **kwargs):
        received.update(args=args, kwargs=kwargs)
        output.mkdir()
        report = {"all_performance_valid": all_valid, "results": [
            {"period": "2026", "model": "LGB", "performance_valid": False, "stopped": "unfunded_tax"}]}
        (output / "report.json").write_text(json.dumps(report))
        return report

    monkeypatch.setattr(validation, "run_stability_validation", validate)
    result = CliRunner().invoke(research_group, ["ashare-stability-validate",
        *arguments(paths, validate=True), "--output-dir", str(output)])
    assert result.exit_code == 1 and "reports preserved" in result.output
    assert "return " not in result.output and (output / "report.json").exists()
    assert received["args"] == (paths["snapshot"], paths["scope-dir"], paths["protocol"],
        paths["market-audit"], paths["predictions-dir"], paths["merger-actions"], output)


def test_valid_validation_summarizes_secondary_results(tmp_path, monkeypatch):
    from stopat30m import ashare_stability_validation as validation
    paths = inputs(tmp_path)

    def validate(*args, **kwargs):
        return {"all_performance_valid": True, "results": [{"period": "2025", "model": "LGB",
            "performance_valid": True, "metrics": {"total_return_lower_exact": ".01",
                "max_drawdown_conservative_exact": ".02"}}]}

    monkeypatch.setattr(validation, "run_stability_validation", validate)
    result = CliRunner().invoke(research_group, ["ashare-stability-validate",
        *arguments(paths, validate=True), "--output-dir", str(tmp_path / "results")])
    assert result.exit_code == 0, result.output
    assert "2025/LGB: return 1.00%; drawdown 2.00%; valid True" in result.output
