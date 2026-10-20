"""Scope command is a lazy, membership-only view over bound source evidence."""
import builtins

from click.testing import CliRunner
import pytest

from stopat30m.ashare_scope_cli import DEFAULT_DECLARATION
from stopat30m.research_cli import research_group


def test_scope_help_does_not_load_market_or_training_runtime(monkeypatch):
    original = builtins.__import__
    def imported(name, *args, **kwargs):
        if name in {"stopat30m.data.ashare_scope", "stopat30m.ashare_experiment", "pandas", "qlib", "lightgbm", "baostock"}:
            pytest.fail("Scope help must remain lazy")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imported)
    result = CliRunner().invoke(research_group, ["ashare-scope-status", "--help"])
    assert result.exit_code == 0, result.output
    assert "--historical-dir" in result.output and "--scope-declaration" in result.output


@pytest.mark.parametrize("derived", [False, True])
@pytest.mark.parametrize("status", ["blocked", "passed"])
def test_scope_command_dispatches_exact_sources_and_discloses_source_date_vs_fetch(tmp_path, monkeypatch, derived, status):
    from stopat30m.data import ashare_scope
    calls = []
    def build(*args, **kwargs):
        calls.append((args, kwargs))
        return {"historical_daily_audit": {"status": status}, "historical_union_security_count": 1744,
            "historical_download_targets": ["SH600000"] if status == "passed" else [],
            "historical_cached_eligible_symbols": ["SH600000"] if status == "passed" else [],
            "current_cached_eligible_symbols": ["SH600000"],
            "current": {"as_of_date": "2026-10-19", "member_count": 1300,
                "observed_at": {"csi300": "2026-10-20T01:59:27+00:00", "csi1000": "2026-10-20T01:59:28+00:00"}}}
    monkeypatch.setattr(ashare_scope, "build_scope_receipt", build)
    output = tmp_path / "scope-output"
    extra = ["--historical-dir", str(tmp_path)] if derived else []
    result = CliRunner().invoke(research_group, ["ashare-scope-status", "--snapshot", str(tmp_path),
        "--start-date", "2023-01-01", "--end-date", "2024-12-31", "--current-dir", str(tmp_path),
        "--output-dir", str(output), *extra])
    assert result.exit_code == 0, result.output
    assert calls == [((tmp_path, "2023-01-01", "2024-12-31", tmp_path, output),
        {"declaration_path": DEFAULT_DECLARATION, "historical_dir": tmp_path if derived else None})]
    assert f"Historical scope audit: {status}" in result.output
    assert "Current source as-of: 2026-10-19; members: 1300" in result.output
    assert "actually fetched at: 2026-10-20" in result.output
    assert "No returns generated" in result.output and "subset" in result.output
    assert not output.exists()


def test_rejected_project_scope_is_a_clear_click_error(tmp_path, monkeypatch):
    from stopat30m.data import ashare_scope
    def fail(*args, **kwargs):
        raise ValueError("Project scope allows only csi300 and csi1000")
    monkeypatch.setattr(ashare_scope, "build_scope_receipt", fail)
    result = CliRunner().invoke(research_group, ["ashare-scope-status", "--snapshot", str(tmp_path),
        "--start-date", "2023-01-01", "--end-date", "2024-12-31", "--current-dir", str(tmp_path),
        "--output-dir", str(tmp_path / "new")])
    assert result.exit_code == 1 and "Error: Project scope allows only" in result.output
    assert "Traceback" not in result.output
