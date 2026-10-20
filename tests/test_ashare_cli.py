"""Pinned base provenance and retired unsafe Qlib entrypoints stay explicit."""
import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner
import pytest

from stopat30m.data import provider
from stopat30m.data import qlib_snapshot
from stopat30m.research_cli import research_group


@pytest.mark.parametrize("source", ["qlib", "qlib+baostock"])
def test_legacy_qlib_concat_is_refused_before_creating_or_downloading(tmp_path, monkeypatch, source):
    monkeypatch.setattr(provider, "_download_qlib_public", lambda *_: pytest.fail("unsafe legacy network"))
    destination = tmp_path / "absent"
    with pytest.raises(ValueError, match="ashare-base"):
        provider.download_cn_data(source=source, target_dir=str(destination))
    assert not destination.exists()


def test_legacy_append_cannot_mutate_a_frozen_dataset(tmp_path):
    root = tmp_path / "version"
    root.mkdir()
    (root / "manifest.json").write_text("{}")
    with pytest.raises(ValueError, match="不可原地更新"):
        provider.download_cn_data(source="baostock", target_dir=str(root / "dataset"))
    assert not (root / "dataset").exists()


@pytest.mark.parametrize("source", ["baostock", "akshare", "efinance", "tushare"])
@pytest.mark.parametrize("existing", [False, True])
def test_unscoped_whole_market_download_cannot_bypass_project_scope(tmp_path, monkeypatch, source, existing):
    destination = tmp_path / "dataset"
    if existing:
        (destination / "calendars").mkdir(parents=True)
        (destination / "calendars/day.txt").write_text("2024-12-31\n")
    before = {str(path.relative_to(destination)): path.read_bytes()
        for path in destination.rglob("*") if path.is_file()} if existing else {}
    monkeypatch.setattr(provider, "_create_fetcher", lambda *_args, **_kwargs: pytest.fail("unscoped source must not start"))
    with pytest.raises(ValueError, match="旧全市场下载/更新入口已停用"):
        provider.download_cn_data(source=source, target_dir=str(destination))
    after = {str(path.relative_to(destination)): path.read_bytes()
        for path in destination.rglob("*") if path.is_file()} if destination.exists() else {}
    assert before == after
    assert destination.exists() == existing


def test_base_command_uses_exact_pinned_source_checksum_and_preserves_classification(tmp_path, monkeypatch):
    source = {"kind": "community", "release_tag": "2026-10-18", "url": "https://example.test/pinned.tar.gz",
              "release_manifest": {"archive_sha256": "a" * 64, "archive_size_bytes": 500}}
    spec = tmp_path / "source.json"
    spec.write_text(json.dumps(source))
    calls = []
    def download(url, path, **kwargs):
        calls.append((url, path, kwargs))
    def prepare(archive, output, **kwargs):
        assert kwargs["source"] == source
        assert kwargs["expected_sha256"] == "a" * 64
        assert kwargs["expected_size_bytes"] == 500
        return SimpleNamespace(dataset_dir=output / "dataset", path=output,
                               manifest={"audit": {"instrument_count": 4000}})
    monkeypatch.setattr(qlib_snapshot, "download_qlib_archive", download)
    monkeypatch.setattr(qlib_snapshot, "prepare_qlib_snapshot", prepare)
    result = CliRunner().invoke(research_group, ["ashare-base", "--source-spec", str(spec),
                                                "--output-dir", str(tmp_path / "base")])
    assert result.exit_code == 0, result.output
    assert calls[0][0] == source["url"] and calls[0][2]["expected_sha256"] == "a" * 64
    assert "Actual security count: 4000" in result.output
    assert "per security" in result.output


def test_base_release_cannot_escape_archive_directory(tmp_path, monkeypatch):
    spec = tmp_path / "source.json"
    spec.write_text(json.dumps({"release_tag": "../../outside", "release_manifest": {}}))
    monkeypatch.setattr(qlib_snapshot, "download_qlib_archive", lambda *_args, **_kwargs: pytest.fail("network"))
    result = CliRunner().invoke(research_group, ["ashare-base", "--source-spec", str(spec),
                                                "--output-dir", str(tmp_path / "base")])
    assert result.exit_code != 0
    assert not (tmp_path / "base").exists()


def test_rebuild_refuses_existing_output_before_loading_or_writing(tmp_path, monkeypatch):
    marker = tmp_path / "preserve.txt"
    marker.write_text("original version")
    monkeypatch.setattr(qlib_snapshot, "load_qlib_snapshot", lambda *_args: pytest.fail("must reject first"))
    result = CliRunner().invoke(research_group, ["ashare-rebuild", "--base-snapshot", str(tmp_path),
        "--collection", str(tmp_path), "--calendar", str(marker), "--failure-audit", str(marker),
        "--output-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert marker.read_text() == "original version"


def test_partial_update_rejects_parent_output_before_writing_diagnostics(tmp_path, monkeypatch):
    frozen = tmp_path / "snapshot"
    dataset = frozen / "dataset"
    dataset.mkdir(parents=True)
    marker = frozen / "manifest.json"
    marker.write_text("{}")
    calendar = tmp_path / "calendar.json"
    calendar.write_text("{}")
    monkeypatch.setattr(qlib_snapshot, "load_qlib_snapshot", lambda *_args:
        SimpleNamespace(path=frozen, dataset_dir=dataset))
    result = CliRunner().invoke(research_group, ["ashare-update", "--base-snapshot", str(frozen),
        "--collection", str(tmp_path), "--calendar", str(calendar), "--base-volume-multiplier", "100",
        "--allow-partial", "--output-dir", str(dataset)])
    assert result.exit_code != 0 and "immutable" in result.output
    assert list(dataset.iterdir()) == []
    assert marker.read_text() == "{}"
