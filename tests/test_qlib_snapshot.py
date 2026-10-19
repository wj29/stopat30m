"""Tiny synthetic archives exercise safety/coverage; no external IO or Qlib."""
import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile

import numpy as np
import pytest

from stopat30m.data import qlib_snapshot as snapshot


DAYS = ["2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"]
SOURCE = {"kind": "community", "release_tag": "fixture-1", "url": "https://example.test/fixture.tar.gz",
          "release_manifest": {"target_trade_date": DAYS[-1], "future_start_date": "2026-10-19",
                               "future_end_date": "2026-10-20"}}


def binary(offset, values):
    return np.asarray([offset, *values], dtype="<f4").tobytes()


@pytest.fixture
def members():
    content = {"qlib_bin/calendars/day.txt": ("\n".join(DAYS) + "\n").encode(),
        "qlib_bin/calendars/day_future.txt": ("\n".join([*DAYS, "2026-10-19", "2026-10-20"]) + "\n").encode(),
        "qlib_bin/instruments/all.txt": b"SH600000\t2026-10-12\t2026-10-16\nSZ000001\t2026-10-14\t2026-10-16\n",
        "qlib_bin/instruments/csi300.txt": b"SH600000\t2026-10-12\t2026-10-13\nSH600000\t2026-10-15\t2026-10-16\nSZ000001\t2026-10-14\t2026-10-16\n"}
    for instrument, offset, prices in [("sh600000", 0, [1, 1.1, np.nan, 1.2, 1.3]),
                                       ("sz000001", 2, [2, 2.1])]:
        for field in snapshot.AUDIT_FIELDS:
            values = list(prices)
            if field == "high":
                values = [value * 1.01 for value in values]
            elif field == "low":
                values = [value * .99 for value in values]
            elif field == "volume":
                values = [1000 if np.isfinite(value) else np.nan for value in values]
            elif field == "factor":
                values = [2 if np.isfinite(value) else np.nan for value in values]
            if field == "vwap" and instrument == "sh600000":
                values[-1] = np.nan
            content[f"qlib_bin/features/{instrument}/{field}.day.bin"] = binary(offset, values)
    return content


def archive(tmp_path, members, *, extra=None):
    target = tmp_path / "fixture.tar.gz"
    with tarfile.open(target, "w:gz") as stream:
        for name, raw in members.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            stream.addfile(member, io.BytesIO(raw))
        if extra is not None:
            stream.addfile(extra, io.BytesIO(b"x" * extra.size) if extra.isfile() else None)
    return target, hashlib.sha256(target.read_bytes()).hexdigest()


def prepare(path, sha, output, **kwargs):
    return snapshot.prepare_qlib_snapshot(path, output, source=SOURCE, expected_sha256=sha,
                                          expected_size_bytes=path.stat().st_size, **kwargs)


def test_snapshot_preserves_archive_and_audits_finite_fields_and_effective_intervals(tmp_path, members):
    path, sha = archive(tmp_path, members)
    original = path.read_bytes()
    result = prepare(path, sha, tmp_path / "base")
    assert result.dataset_dir == tmp_path / "base/snapshots" / sha / "dataset"
    assert path.read_bytes() == original
    assert result.manifest["source"]["kind"] == "community"
    assert result.manifest["execution_ready"] is False
    audit = result.manifest["audit"]
    assert audit["calendar"] == {"start": DAYS[0], "end": DAYS[-1], "session_count": 5,
                                  "future_start": "2026-10-19", "future_end": "2026-10-20"}
    stock = audit["instruments"]["SH600000"]
    assert stock["fields"]["close"]["last_finite_date"] == "2026-10-16"
    assert stock["fields"]["vwap"]["last_finite_date"] == "2026-10-15"
    assert stock["last_complete_bar_date"] == "2026-10-15"
    assert stock["complete_bar_count"] == 3
    assert audit["instruments"]["SZ000001"]["close_missing_at_calendar_end"] is True
    assert audit["complete_bar_tail_counts"] == {"2026-10-15": 2}
    membership = audit["instrument_files"]["csi300"]
    assert membership["interval_count"] == 3 and membership["instrument_count"] == 2
    assert membership["effective_ranges_only"] is True and membership["announcement_pit_verified"] is False
    assert result.manifest["files"][membership["path"]]["sha256"] == membership["sha256"]
    assert result.manifest["price_semantics"]["volume_unit_verified"] is False
    reloaded = snapshot.load_qlib_snapshot(result.path / "manifest.json")
    assert reloaded.manifest == result.manifest and reloaded.manifest_sha256 == result.manifest_sha256
    before = (result.path / "manifest.json").read_bytes()
    assert prepare(path, sha, tmp_path / "base").manifest_sha256 == result.manifest_sha256
    assert (result.path / "manifest.json").read_bytes() == before


@pytest.mark.parametrize("change", ["edited", "extra", "symlink"])
def test_published_snapshot_rejects_changed_or_escaped_dataset(tmp_path, members, change):
    path, sha = archive(tmp_path, members)
    result = prepare(path, sha, tmp_path / "base")
    field = result.dataset_dir / "features/sh600000/close.day.bin"
    if change == "edited":
        field.write_bytes(binary(0, [9, 9, 9, 9, 9]))
    elif change == "extra":
        (result.dataset_dir / "unexpected.txt").write_text("extra")
    else:
        field.unlink()
        field.symlink_to(path)
    with pytest.raises(snapshot.QlibSnapshotError):
        snapshot.load_qlib_snapshot(result.path)


@pytest.mark.parametrize("name", ["../escape", "/absolute", "qlib_bin/../../escape", "C:\\escape"])
def test_archive_path_traversal_never_publishes_a_snapshot(tmp_path, members, name):
    member = tarfile.TarInfo(name)
    member.size = 1
    path, sha = archive(tmp_path, members, extra=member)
    with pytest.raises(snapshot.QlibSnapshotError, match="Unsafe"):
        prepare(path, sha, tmp_path / "base")
    assert not (tmp_path / "escape").exists()
    assert not (tmp_path / "base/snapshots" / sha).exists()


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.CHRTYPE])
def test_archive_symlinks_hardlinks_and_devices_are_not_extracted(tmp_path, members, kind):
    member = tarfile.TarInfo("qlib_bin/features/evil")
    member.type, member.linkname = kind, "../../escape"
    path, sha = archive(tmp_path, members, extra=member)
    with pytest.raises(snapshot.QlibSnapshotError, match="links/devices"):
        prepare(path, sha, tmp_path / "base")
    assert not (tmp_path / "base/snapshots" / sha).exists()


def test_size_hash_duplicate_member_and_expansion_budget_fail_closed(tmp_path, members):
    path, sha = archive(tmp_path, members)
    with pytest.raises(snapshot.QlibSnapshotError, match="SHA"):
        prepare(path, "0" * 64, tmp_path / "wrong-sha")
    with pytest.raises(snapshot.QlibSnapshotError, match="size"):
        snapshot.prepare_qlib_snapshot(path, tmp_path / "wrong-size", source=SOURCE,
                                       expected_sha256=sha, expected_size_bytes=1)
    with pytest.raises(snapshot.QlibSnapshotError, match="budget"):
        prepare(path, sha, tmp_path / "limited", max_extract_bytes=50)
    duplicate = tarfile.TarInfo("qlib_bin/calendars/day.txt")
    duplicate.size = 1
    path, sha = archive(tmp_path, members, extra=duplicate)
    with pytest.raises(snapshot.QlibSnapshotError, match="Duplicate"):
        prepare(path, sha, tmp_path / "duplicate")


@pytest.mark.parametrize("offset,values", [(-1, [1]), (.5, [1]), (np.nan, [1]),
                                          (4, [1, 2]), (0, [1, np.inf])])
def test_binary_offsets_overflow_and_infinity_are_not_silently_clipped(tmp_path, offset, values):
    path = tmp_path / "close.day.bin"
    path.write_bytes(binary(offset, values))
    with pytest.raises(snapshot.QlibSnapshotError):
        snapshot.read_qlib_field(path, DAYS)


def test_binary_reader_preserves_offset_nan_and_trailing_unavailable_days(tmp_path):
    path = tmp_path / "close.day.bin"
    path.write_bytes(binary(1, [1, np.nan, 3]))
    values = snapshot.read_qlib_field(path, DAYS)
    np.testing.assert_allclose(values, [np.nan, 1, np.nan, 3, np.nan], equal_nan=True)


def test_historical_alphanumeric_alias_is_preserved_and_disclosed_not_guessed(tmp_path, members):
    members["qlib_bin/instruments/csi300.txt"] += b"SHT00018\t2005-04-08\t2006-08-14\n"
    path, sha = archive(tmp_path, members)
    result = prepare(path, sha, tmp_path / "base")
    membership = result.manifest["audit"]["instrument_files"]["csi300"]
    assert membership["non_numeric_instruments"] == ["SHT00018"]
    assert (result.dataset_dir / membership["path"]).read_bytes().endswith(b"SHT00018\t2005-04-08\t2006-08-14\n")


def test_inconsistent_ohlc_does_not_count_as_a_complete_bar(tmp_path, members):
    members["qlib_bin/features/sh600000/vwap.day.bin"] = binary(0, [1, 1.1, np.nan, 1.2, 1.3])
    members["qlib_bin/features/sh600000/high.day.bin"] = binary(0, [1.01, 1.111, np.nan, 1.212, .5])
    path, sha = archive(tmp_path, members)
    result = prepare(path, sha, tmp_path / "base")
    stock = result.manifest["audit"]["instruments"]["SH600000"]
    assert stock["last_complete_bar_date"] == "2026-10-15"
    assert "inconsistent_ohlc:1" in stock["quality_issues"]


def test_source_release_checksum_cannot_disagree_with_the_requested_archive(tmp_path, members):
    path, sha = archive(tmp_path, members)
    source = {**SOURCE, "release_manifest": {**SOURCE["release_manifest"], "archive_sha256": "sha256:" + "0" * 64}}
    with pytest.raises(snapshot.QlibSnapshotError, match="source declaration"):
        snapshot.prepare_qlib_snapshot(path, tmp_path / "base", source=source, expected_sha256=sha)
    assert not (tmp_path / "base").exists()


def bridge_fixture(tmp_path, members):
    members["qlib_bin/features/sh600000/vwap.day.bin"] = binary(0, [1, 1.1, np.nan, 1.2, 1.3])
    path, sha = archive(tmp_path, members)
    parent = prepare(path, sha, tmp_path / "base")
    bridge = tmp_path / "bridge"
    shutil.copytree(parent.dataset_dir, bridge)
    (bridge / "calendars/day.txt").write_text("\n".join([*DAYS, "2026-10-19"]) + "\n")
    for field in snapshot.AUDIT_FIELDS:
        file = bridge / "features/sh600000" / (field + ".day.bin")
        before = np.frombuffer(file.read_bytes(), dtype="<f4")
        file.write_bytes(np.append(before, before[-1]).astype("<f4").tobytes())
    increment = {"schema_version": 1, "status": "bridge_verified", "base_unchanged": True,
        "base_dir": str(parent.dataset_dir), "source": "offline_bridge_fixture", "generation": "f" * 32,
        "source_evidence": {"synthetic": True}, "scope": "base_clone_with_audited_security_updates",
        "not_a_full_market_update": True, "audited_common_finite_end": "2026-10-19",
        "dataset_files": {str(file.relative_to(bridge)): hashlib.sha256(file.read_bytes()).hexdigest()
                          for file in bridge.rglob("*") if file.is_file()}}
    seal_increment(bridge, increment)
    return parent, bridge, increment


def seal_increment(bridge, increment):
    unsigned = {key: value for key, value in increment.items() if key != "version_sha256"}
    increment["version_sha256"] = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
                                               separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    (bridge / "incremental_manifest.json").write_text(json.dumps(increment, sort_keys=True, indent=2) + "\n")


def freeze_bridge(tmp_path, parent, bridge):
    return snapshot.prepare_qlib_directory(bridge, tmp_path / "master", source=SOURCE,
        parent_snapshot=parent, increment_manifest_path=bridge / "incremental_manifest.json")


def test_incremental_directory_gets_its_own_identity_and_preserves_per_security_watermarks(tmp_path, members):
    parent, bridge, _ = bridge_fixture(tmp_path, members)
    parent_bytes = (parent.path / "manifest.json").read_bytes()
    child = freeze_bridge(tmp_path, parent, bridge)
    assert child.manifest["kind"] == "qlib_cn_incremental_snapshot"
    assert "archive" not in child.manifest
    assert child.manifest["snapshot_id"] != parent.manifest["snapshot_id"]
    assert child.manifest["lineage"]["parent_snapshot"] == {
        "snapshot_id": parent.manifest["snapshot_id"], "manifest_sha256": parent.manifest_sha256}
    assert child.manifest["lineage"]["increment"]["source"] == "offline_bridge_fixture"
    assert child.manifest["audit"]["calendar"]["end"] == "2026-10-19"
    assert child.manifest["audit"]["instruments"]["SH600000"]["last_complete_bar_date"] == "2026-10-19"
    assert child.manifest["audit"]["instruments"]["SZ000001"]["last_complete_bar_date"] == "2026-10-15"
    assert (parent.path / "manifest.json").read_bytes() == parent_bytes
    assert snapshot.load_qlib_snapshot(child.path).manifest_sha256 == child.manifest_sha256
    assert freeze_bridge(tmp_path, parent.path, bridge).manifest_sha256 == child.manifest_sha256


@pytest.mark.parametrize("defect", ["edited_data", "bad_manifest", "wrong_parent", "unverified_status", "symlink"])
def test_incremental_publication_checks_bridge_bytes_version_parent_and_source_status(tmp_path, members, defect):
    parent, bridge, increment = bridge_fixture(tmp_path, members)
    if defect == "edited_data":
        (bridge / "features/sh600000/close.day.bin").write_bytes(binary(0, [9] * 6))
    elif defect == "bad_manifest":
        increment["generation"] = "changed-without-checksum"
        (bridge / "incremental_manifest.json").write_text(json.dumps(increment))
    elif defect == "wrong_parent":
        increment["base_dir"] = str(tmp_path / "not-the-parent")
        seal_increment(bridge, increment)
    elif defect == "unverified_status":
        increment["status"] = "failed"
        seal_increment(bridge, increment)
    else:
        field = bridge / "features/sh600000/close.day.bin"
        field.unlink()
        field.symlink_to(parent.dataset_dir / "features/sh600000/close.day.bin")
    with pytest.raises(snapshot.QlibSnapshotError):
        freeze_bridge(tmp_path, parent, bridge)
    assert not (tmp_path / "master").exists()


@pytest.mark.parametrize("marker", ["manifest.json", "incremental_manifest.json", "raw_qlib_layout"])
def test_guard_rejects_artifact_ancestors_before_creating_any_output_or_lock(tmp_path, marker):
    frozen = tmp_path / "frozen"
    frozen.mkdir()
    if marker == "raw_qlib_layout":
        for name in ("features", "calendars", "instruments"):
            (frozen / name).mkdir()
    else:
        # Invalid JSON deliberately proves the guard needs no content parsing.
        (frozen / marker).write_text("not json; do not parse marker content")
    before = sorted(str(path.relative_to(frozen)) for path in frozen.rglob("*"))
    output = frozen / "new-output" / "nested"
    with pytest.raises(snapshot.QlibSnapshotError, match="outside"):
        snapshot.guard_qlib_output_path(output)
    assert not (frozen / "new-output").exists()
    assert sorted(str(path.relative_to(frozen)) for path in frozen.rglob("*")) == before


@pytest.mark.parametrize("output_kind", ["parent_root", "parent_dataset", "different_artifact", "symlink_alias"])
def test_directory_registration_cannot_pollute_its_parent_or_another_frozen_artifact(tmp_path, members, output_kind):
    parent, bridge, _ = bridge_fixture(tmp_path, members)
    other = tmp_path / "another-frozen-artifact"
    other.mkdir()
    (other / "incremental_manifest.json").write_text("marker only")
    choices = {"parent_root": parent.path / "new-versions",
               "parent_dataset": parent.dataset_dir / "new-versions",
               "different_artifact": other / "new-versions"}
    if output_kind == "symlink_alias":
        alias = tmp_path / "alias"
        alias.symlink_to(parent.dataset_dir, target_is_directory=True)
        output = alias / "new-versions"
    else:
        output = choices[output_kind]
    before = {str(path.relative_to(parent.path)): path.read_bytes()
              for path in parent.path.rglob("*") if path.is_file()}
    with pytest.raises(snapshot.QlibSnapshotError, match="outside"):
        snapshot.prepare_qlib_directory(bridge, output, source=SOURCE, parent_snapshot=parent,
            increment_manifest_path=bridge / "incremental_manifest.json")
    assert not output.exists()
    assert {str(path.relative_to(parent.path)): path.read_bytes()
            for path in parent.path.rglob("*") if path.is_file()} == before
    assert not (other / "new-versions").exists()
    assert snapshot.load_qlib_snapshot(parent.path).manifest_sha256 == parent.manifest_sha256


@pytest.mark.parametrize("marker", ["manifest.json", "incremental_manifest.json"])
def test_archive_prepare_cannot_create_locks_or_versions_inside_another_artifact(tmp_path, members, marker):
    path, sha = archive(tmp_path, members)
    frozen = tmp_path / "original-artifact"
    frozen.mkdir()
    (frozen / marker).write_text("existing marker")
    with pytest.raises(snapshot.QlibSnapshotError, match="outside"):
        prepare(path, sha, frozen / "new-base")
    assert sorted(item.name for item in frozen.iterdir()) == [marker]


class Response:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status
        self.headers = {"Content-Length": str(len(payload))}
    def __enter__(self):
        return self
    def __exit__(self, *_args):
        pass
    def iter_content(self, *_args):
        yield self.payload[:3]
        yield self.payload[3:]


class Session:
    def __init__(self, response):
        self.response, self.calls = response, []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_stream_download_checks_bytes_and_reuses_only_exact_archive(tmp_path):
    payload = b"synthetic archive bytes"
    sha = hashlib.sha256(payload).hexdigest()
    session = Session(Response(payload))
    path = tmp_path / "archive.tar.gz"
    kwargs = dict(expected_sha256=sha, expected_size_bytes=len(payload), session=session)
    assert snapshot.download_qlib_archive("https://example.test/pinned/archive", path, **kwargs) == path
    assert path.read_bytes() == payload
    snapshot.download_qlib_archive("https://example.test/pinned/archive", path, **kwargs)
    assert len(session.calls) == 1
    path.write_bytes(b"bad")
    with pytest.raises(snapshot.QlibSnapshotError, match="Existing archive"):
        snapshot.download_qlib_archive("https://example.test/pinned/archive", path, **kwargs)
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [403, 429, 503])
def test_failed_archive_download_does_not_retry_or_publish(tmp_path, status):
    payload = b"provider error"
    session = Session(Response(payload, status))
    target = tmp_path / "archive.tar.gz"
    with pytest.raises(snapshot.QlibSnapshotError, match=f"HTTP status {status}"):
        snapshot.download_qlib_archive("https://example.test/pinned/archive", target,
            expected_sha256=hashlib.sha256(payload).hexdigest(), expected_size_bytes=len(payload), session=session)
    assert len(session.calls) == 1 and not target.exists()


@pytest.mark.parametrize("marker", ["manifest.json", "incremental_manifest.json"])
def test_download_refuses_frozen_ancestor_before_network_or_mkdir(tmp_path, marker):
    frozen = tmp_path / "original-artifact"
    frozen.mkdir()
    (frozen / marker).write_text("existing marker")
    payload = b"fixture bytes"
    session = Session(Response(payload))
    target = frozen / "archives" / "new-base.tar.gz"
    with pytest.raises(snapshot.QlibSnapshotError, match="outside"):
        snapshot.download_qlib_archive("https://example.test/pinned/archive", target,
            expected_sha256=hashlib.sha256(payload).hexdigest(), expected_size_bytes=len(payload), session=session)
    assert session.calls == [] and not target.parent.exists()
    assert sorted(item.name for item in frozen.iterdir()) == [marker]
