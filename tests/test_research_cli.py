"""Lock real experiment declarations without fetching or looking at final OOS."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from stopat30m.backtest.walk_forward import load_protocol
from stopat30m.data import research_prices
from stopat30m.research_cli import DEFAULT_SPEC, research_group


def experiment(tmp_path):
    spec = json.loads(DEFAULT_SPEC.read_text())
    spec_path = tmp_path / "experiment.json"
    spec_path.write_text(json.dumps(spec))
    protocol_path = tmp_path / "protocol.json"
    args = ["--spec", str(spec_path), "--protocol", str(protocol_path)]
    return spec, spec_path, protocol_path, args


def test_lock_is_offline_and_retries_preserve_actual_lock_time(tmp_path, monkeypatch):
    monkeypatch.setattr(research_prices, "download_research_prices", lambda *a, **kw: pytest.fail("lock must not inspect prices"))
    _, _, path, args = experiment(tmp_path)
    cli = CliRunner()
    assert cli.invoke(research_group, ["lock", *args]).exit_code == 0
    original = path.read_bytes()
    assert load_protocol(path)["universe"]["metadata"]["research_data_request"]["adjustment"] == "hfq"
    assert cli.invoke(research_group, ["lock", *args]).exit_code == 0
    assert path.read_bytes() == original


@pytest.mark.parametrize("mutation", ["parameters", "pool", "source", "adjustment", "selection_date", "coverage"])
def test_cli_cannot_relabel_or_modify_a_locked_experiment(tmp_path, mutation):
    spec, spec_path, path, args = experiment(tmp_path)
    cli = CliRunner()
    assert cli.invoke(research_group, ["lock", *args]).exit_code == 0
    original = path.read_bytes()
    if mutation == "parameters":
        spec["config"]["top_k"] = 2
    elif mutation == "pool":
        spec["universe"]["instruments"] = ["SH510300"]
    elif mutation == "selection_date":
        spec["universe"]["selection_date"] = "2020-01-01"
    elif mutation == "coverage":
        spec["data_request"]["start_date"] = "2012-01-01"
    else:
        spec["data_request"][mutation] = "different_source" if mutation == "source" else "qfq"
    spec_path.write_text(json.dumps(spec))
    result = cli.invoke(research_group, ["lock", *args])
    assert result.exit_code == 1, result.output
    assert path.read_bytes() == original


def test_failed_network_download_leaves_a_valid_preexisting_protocol(tmp_path, monkeypatch):
    _, _, path, args = experiment(tmp_path)

    def failed_download(*a, **kw):
        assert path.exists()
        load_protocol(path)
        raise research_prices.ResearchDataError("offline-test provider unavailable")

    monkeypatch.setattr(research_prices, "download_research_prices", failed_download)
    monkeypatch.setattr(research_prices, "EastmoneyResearchProvider", lambda **kw: SimpleNamespace(close=lambda: None))
    result = CliRunner().invoke(research_group, ["prepare", *args, "--cache-dir", str(tmp_path / "cache")])
    assert result.exit_code == 1
    assert "provider unavailable" in result.output
    assert load_protocol(path)["config"]["lookback_sessions"] == 60
    failures = list((tmp_path / "cache/failures").glob("*.json"))
    assert len(failures) == 1
    assert json.loads(failures[0].read_text())["usable_snapshot_created"] is False
