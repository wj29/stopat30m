from click.testing import CliRunner
import click
import json

from stopat30m.ashare_union_cli import register_commands


@click.group()
def research():
    pass


register_commands(research)


def test_union_commands_expose_fixed_research_inputs_without_starting_work():
    runner = CliRunner()
    for command in ("ashare-union-freeze", "ashare-union-train", "ashare-union-validate"):
        result = runner.invoke(research, [command, "--help"])
        assert result.exit_code == 0
        assert "--snapshot" in result.output and "--scope-dir" in result.output


def test_incomplete_union_train_request_cannot_start_fitting():
    result = CliRunner().invoke(research, ["ashare-union-train"])
    assert result.exit_code == 2 and "Missing option" in result.output


def test_union_audit_has_separate_source_only_entry_point():
    result = CliRunner().invoke(research, ["ashare-union-audit", "--help"])
    assert result.exit_code == 0 and "--bundle" in result.output


def test_audit_preserves_explicit_correction_receipts_and_archive_view(tmp_path, monkeypatch):
    from stopat30m.data import a5_market_audit
    received = {}

    def audit(*args, **kwargs):
        received.update(kwargs)
        return {"status": "passed", "raw_rows": 1, "event_count": 0,
                "source_review_items": 0, "checks": {"source_vintage": True}}

    monkeypatch.setattr(a5_market_audit, "build_a5_market_audit", audit)
    request = {"kind": "a5_union_market_audit_request", "snapshot": "snapshot",
        "scope_directory": "scope", "execution_collections": ["raw"],
        "dividend_collections": ["dividends"], "start": "2023-01-03", "end": "2024-12-31",
        "official_membership_evidence": "membership.json", "archive_scope_view": True,
        "dividend_source_correction_receipt_paths": ["correction.json"],
        "adjustment_bridge_spec_path": "bridge.json", "partial_session_model_policy_path": "partial.json"}
    bundle = tmp_path / "request.json"
    bundle.write_text(json.dumps(request))
    result = CliRunner().invoke(research, ["ashare-union-audit", "--bundle", str(bundle),
        "--output-dir", str(tmp_path / "audit")])
    assert result.exit_code == 0, result.output
    assert received["archive_scope_view"] is True
    assert received["dividend_source_correction_receipt_paths"] == [tmp_path / "correction.json"]
    assert received["adjustment_bridge_spec_path"] == tmp_path / "bridge.json"
    assert received["partial_session_model_policy_path"] == tmp_path / "partial.json"
    assert callable(received["progress"])
    request["archive_scope_view"] = "false"
    bundle.write_text(json.dumps(request))
    received.clear()
    result = CliRunner().invoke(research, ["ashare-union-audit", "--bundle", str(bundle),
        "--output-dir", str(tmp_path / "audit")])
    assert result.exit_code != 0 and "explicit JSON boolean" in result.output
    assert not received
