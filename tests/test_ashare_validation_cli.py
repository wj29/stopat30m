"""Audit/declare/validate commands remain lazy and dispatch exact bound paths."""
import builtins
from pathlib import Path

from click.testing import CliRunner
import pytest

from stopat30m.research_cli import research_group
from stopat30m.ashare_validation_cli import DEFAULT_SPEC


@pytest.mark.parametrize("command", ["ashare-history-audit", "ashare-validation-declare", "ashare-validate-history"])
def test_help_does_not_import_research_or_market_runtime(monkeypatch, command):
    original = builtins.__import__
    blocked = {"stopat30m.ashare_validation", "stopat30m.data.ashare_history_audit", "qlib", "lightgbm", "baostock", "pandas"}
    def imported(name, *args, **kwargs):
        if name in blocked:
            pytest.fail("Command help must not import a research/data runtime")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imported)
    result = CliRunner().invoke(research_group, [command, "--help"])
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize("with_rights", [False, True])
def test_history_audit_dispatch_passes_correct_api_names_and_shows_blocked_counts(tmp_path, monkeypatch, with_rights):
    from stopat30m.data import ashare_history_audit
    calls = []
    def audit(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "blocked", "checks": {"raw_ohlc": True, "dividend_events": False},
            "raw_rows": 169479, "event_count": 900, "unexplained_references": 3,
            "missing_stock_dividend_tax_evidence": ["bonus-1", "bonus-2"]}
    monkeypatch.setattr(ashare_history_audit, "build_historical_market_audit", audit)
    overrides, rights = tmp_path / "overrides.json", tmp_path / "rights.json"
    observations, capital_tax = tmp_path / "intraday.json", tmp_path / "capital-tax.json"
    overrides.write_text("{}")
    rights.write_text("{}")
    observations.write_text("{}")
    capital_tax.write_text("{}")
    output = tmp_path / "new-audit"
    options = ["--action-overrides", str(overrides), "--rights-events", str(rights),
        "--intraday-observations", str(observations), "--capital-reserve-tax-evidence", str(capital_tax)] if with_rights else []
    result = CliRunner().invoke(research_group, ["ashare-history-audit", "--snapshot", str(tmp_path),
        "--execution-collection", str(tmp_path), "--dividends-collection", str(tmp_path), "--output-dir", str(output), *options])
    assert result.exit_code == 0, result.output
    assert calls == [((tmp_path, tmp_path, tmp_path, output), {
        "action_overrides_path": overrides if with_rights else None,
        "rights_events_path": rights if with_rights else None,
        "intraday_observations_path": observations if with_rights else None,
        "capital_reserve_tax_evidence_path": capital_tax if with_rights else None})]
    assert "Historical market audit: blocked" in result.output
    assert "Blocked checks: 1; unexplained references: 3; missing stock-dividend tax evidence: 2" in result.output
    assert "Failed checks: dividend_events" in result.output
    assert "no portfolio returns or strategy selection generated" in result.output
    assert not output.exists()


@pytest.mark.parametrize("explicit_spec", [False, True])
def test_validation_declare_dispatch_freezes_only_the_original_or_explicit_spec(tmp_path, monkeypatch, explicit_spec):
    from stopat30m import ashare_validation
    calls = []
    def declare(*args, **kwargs):
        calls.append((args, kwargs))
        return {"declared_at": "2026-10-20T01:00:00+00:00", "a3_spec_sha256": "a" * 64}
    monkeypatch.setattr(ashare_validation, "write_validation_declaration", declare)
    spec, output = tmp_path / "spec.json", tmp_path / "declaration.json"
    spec.write_text("{}")
    extra = ["--spec", str(spec)] if explicit_spec else []
    result = CliRunner().invoke(research_group, ["ashare-validation-declare", "--output", str(output), *extra])
    assert result.exit_code == 0, result.output
    assert calls == [((output,), {"spec_path": spec if explicit_spec else DEFAULT_SPEC})]
    assert "No returns calculated" in result.output and "active paper period are unchanged" in result.output
    assert not output.exists()


def test_validation_dispatch_reports_exact_bounds_and_selection_without_switching(tmp_path, monkeypatch):
    from stopat30m import ashare_validation
    calls = []
    def validate(*args, **kwargs):
        calls.append((args, kwargs))
        return {"cases": {"validation/M0_E1": {"metrics": {"total_return_lower_exact": "0.10",
            "total_return_upper_exact": "0.1001", "max_drawdown_conservative_exact": "-0.15",
            "accounting_invalid_days": 0, "terminal_prices_fresh": True}}},
            "selection_status": "validated_candidate_recommended_pending_separate_forward_freeze", "selected_candidate": "M0_E1"}
    monkeypatch.setattr(ashare_validation, "run_validation", validate)
    bundle, declaration = tmp_path / "bundle.json", tmp_path / "declaration.json"
    bundle.write_text("{}")
    declaration.write_text("{}")
    output = tmp_path / "never-created"
    result = CliRunner().invoke(research_group, ["ashare-validate-history", "--bundle", str(bundle),
        "--declaration", str(declaration), "--output-dir", str(output)])
    assert result.exit_code == 0, result.output
    assert calls[0][0] == (bundle, output) and calls[0][1]["declaration_path"] == declaration
    assert callable(calls[0][1]["progress"])
    assert "net return bounds 10.00%..10.01%; conservative drawdown -15.00%" in result.output
    assert "Recommended candidate: M0_E1" in result.output and "active paper period is unchanged" in result.output
    assert not output.exists()


@pytest.mark.parametrize("command", ["ashare-history-audit", "ashare-validation-declare", "ashare-validate-history"])
def test_runtime_failures_are_clear_click_errors_without_tracebacks(tmp_path, monkeypatch, command):
    from stopat30m import ashare_validation
    from stopat30m.data import ashare_history_audit
    def fail(*args, **kwargs):
        raise ValueError("Required original source evidence is incomplete")
    monkeypatch.setattr(ashare_history_audit, "build_historical_market_audit", fail)
    monkeypatch.setattr(ashare_validation, "write_validation_declaration", fail)
    monkeypatch.setattr(ashare_validation, "run_validation", fail)
    source = tmp_path / "source.json"
    source.write_text("{}")
    options = {"ashare-history-audit": ["--snapshot", str(tmp_path), "--execution-collection", str(tmp_path),
        "--dividends-collection", str(tmp_path), "--output-dir", str(tmp_path / "new")],
        "ashare-validation-declare": ["--output", str(tmp_path / "declaration.json")],
        "ashare-validate-history": ["--bundle", str(source), "--declaration", str(source), "--output-dir", str(tmp_path / "new")]}
    result = CliRunner().invoke(research_group, [command, *options[command]])
    assert result.exit_code == 1
    assert "Error: Required original source evidence is incomplete" in result.output
    assert "Traceback" not in result.output


def test_missing_required_declaration_output_fails_before_runtime_import(monkeypatch):
    original = builtins.__import__
    def imported(name, *args, **kwargs):
        if name == "stopat30m.ashare_validation":
            pytest.fail("Invalid options must not load historical validation")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imported)
    result = CliRunner().invoke(research_group, ["ashare-validation-declare"])
    assert result.exit_code == 2 and "Missing option '--output'" in result.output
