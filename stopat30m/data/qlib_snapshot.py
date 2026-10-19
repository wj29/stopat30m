"""Immutable Qlib-format bases, with archive safety and actual binary coverage.

This module reads the public binary format without importing or configuring
Qlib. A community release stays community data: publication dates, calendar
tails and index membership ranges do not establish complete bars, point-in-time
announcements or permission to redistribute the underlying market data.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import fcntl
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile
import tempfile
from urllib.parse import urlsplit
import zipfile

import numpy as np

REQUIRED_FIELDS = ("open", "high", "low", "close", "volume", "vwap")
AUDIT_FIELDS = (*REQUIRED_FIELDS, "factor")
_SYMBOL = re.compile(r"^(SH|SZ|BJ)[A-Z0-9]{6}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")


class QlibSnapshotError(ValueError):
    pass


def guard_qlib_output_path(path):
    """Reject writes within an existing immutable artifact, before any mkdir.

    Only filenames/directory markers are examined, never configuration or
    manifest contents. Resolve aliases first so a symlink cannot redirect a
    nominally separate output into a parent dataset.
    """
    target = Path(path).expanduser().resolve()
    for ancestor in (target, *target.parents):
        markers = [ancestor / name for name in ("manifest.json", "incremental_manifest.json")]
        if any(marker.exists() or marker.is_symlink() for marker in markers):
            raise QlibSnapshotError("Output must be outside existing immutable artifact directories")
        if all((ancestor / name).is_dir() for name in ("calendars", "features", "instruments")):
            raise QlibSnapshotError("Output must be outside existing Qlib dataset directories")
    return target


def _bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def _sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _write(path, value):
    path.write_bytes(_bytes(value))


def _digest(value):
    return hashlib.sha256(_bytes(value)).hexdigest()


def _relative(name):
    if not isinstance(name, str) or "\\" in name or ":" in name or "\x00" in name:
        raise QlibSnapshotError("Unsafe archive member path")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        raise QlibSnapshotError("Unsafe archive member path")
    return path


@contextmanager
def _lock(root):
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".qlib-snapshot.lock"
    if lock.is_symlink():
        raise QlibSnapshotError("Snapshot lock must not be a symlink")
    with lock.open("a+b") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise QlibSnapshotError("Another Qlib snapshot operation is running") from None
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _extract(archive, target, max_extract_bytes, max_members):
    """Never call extractall: links/devices/path escapes are never materialized."""
    names, total = set(), 0

    def destination(name, size, regular):
        nonlocal total
        relative = _relative(name)
        if not relative.parts:
            return None
        key = str(relative).casefold()
        if regular and key in names:
            raise QlibSnapshotError("Duplicate archive member")
        names.add(key)
        if len(names) > max_members or size < 0:
            raise QlibSnapshotError("Archive member budget exceeded")
        total += size
        if total > max_extract_bytes:
            raise QlibSnapshotError("Archive expansion budget exceeded")
        result = target.joinpath(*relative.parts)
        result.parent.mkdir(parents=True, exist_ok=True)
        return result

    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as stream:
            for member in stream.infolist():
                mode = member.external_attr >> 16
                if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) not in {0, stat.S_IFREG, stat.S_IFDIR}):
                    raise QlibSnapshotError("Archive links/devices are not permitted")
                path = destination(member.filename, member.file_size, not member.is_dir())
                if path is None:
                    continue
                if member.is_dir():
                    path.mkdir(exist_ok=True)
                else:
                    with stream.open(member) as source, path.open("xb") as output:
                        shutil.copyfileobj(source, output, length=2 ** 20)
    else:
        with tarfile.open(archive, "r:*") as stream:
            for member in stream:
                if not (member.isfile() or member.isdir()):
                    raise QlibSnapshotError("Archive links/devices are not permitted")
                path = destination(member.name, member.size, member.isfile())
                if path is None:
                    continue
                if member.isdir():
                    path.mkdir(exist_ok=True)
                else:
                    with stream.extractfile(member) as source, path.open("xb") as output:
                        shutil.copyfileobj(source, output, length=2 ** 20)
    return total


def _calendar(path):
    days = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    try:
        if not days or any(date.fromisoformat(day).isoformat() != day for day in days):
            raise ValueError
    except ValueError:
        raise QlibSnapshotError("Invalid Qlib calendar dates") from None
    if days != sorted(set(days)):
        raise QlibSnapshotError("Qlib calendar must be sorted and unique")
    return days


def read_qlib_field(path, calendar):
    """Return calendar-aligned float64 values; preserve every missing bar.

    Qlib's first little-endian float32 is the start-calendar index. Remaining
    float32s are observations. Negative/noninteger offsets, infinities and
    overflow beyond the real calendar are malformed, never silently clipped.
    """
    path = Path(path)
    if path.is_symlink() or path.stat().st_size < 4 or path.stat().st_size % 4:
        raise QlibSnapshotError("Malformed Qlib binary field")
    stored = np.fromfile(path, dtype="<f4")
    offset = float(stored[0])
    if not math.isfinite(offset) or offset < 0 or offset != int(offset):
        raise QlibSnapshotError("Invalid Qlib binary calendar offset")
    offset = int(offset)
    values = stored[1:].astype(float)
    if offset + len(values) > len(calendar) or np.isinf(values).any():
        raise QlibSnapshotError("Qlib binary exceeds calendar or contains infinite prices")
    result = np.full(len(calendar), np.nan)
    result[offset:offset + len(values)] = values
    return result


def _field_audit(values, days, field):
    finite = np.isfinite(values)
    valid = finite & (values >= 0 if field == "volume" else values > 0)
    positions, valid_positions = np.flatnonzero(finite), np.flatnonzero(valid)
    return {"finite_count": int(finite.sum()), "valid_count": int(valid.sum()),
            "invalid_finite_count": int((finite & ~valid).sum()),
            "first_finite_date": days[positions[0]] if len(positions) else None,
            "last_finite_date": days[positions[-1]] if len(positions) else None,
            "last_valid_date": days[valid_positions[-1]] if len(valid_positions) else None,
            "missing_count": int((~finite).sum())}


def audit_qlib_directory(dataset_dir):
    """Audit actual per-security finite bars, not advertised directory dates."""
    root = Path(dataset_dir).resolve()
    days = _calendar(root / "calendars/day.txt")
    future_path = root / "calendars/day_future.txt"
    future = _calendar(future_path) if future_path.exists() else []
    if future and future[:len(days)] != days:
        raise QlibSnapshotError("Future calendar does not extend the historical calendar")
    interval_files = {}
    for path in sorted((root / "instruments").glob("*.txt")):
        rows = []
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) != 3 or not _SYMBOL.fullmatch(parts[0].upper()):
                raise QlibSnapshotError("Invalid Qlib security membership interval")
            instrument, start, end = parts[0].upper(), parts[1], parts[2]
            try:
                if date.fromisoformat(start).isoformat() != start or date.fromisoformat(end).isoformat() != end or start > end:
                    raise ValueError
            except ValueError:
                raise QlibSnapshotError("Invalid Qlib security interval dates") from None
            rows.append((instrument, start, end))
        interval_files[path.stem] = {"path": str(path.relative_to(root)), "sha256": _sha(path),
            "interval_count": len(rows), "instrument_count": len({row[0] for row in rows}),
            "non_numeric_instruments": sorted({row[0] for row in rows if not row[0][2:].isdigit()}),
            "first_effective_date": min((row[1] for row in rows), default=None),
            "last_effective_date": max((row[2] for row in rows), default=None),
            "effective_ranges_only": True, "announcement_pit_verified": False}
    instruments, all_fields = {}, set()
    for location in sorted((root / "features").iterdir()):
        if not location.is_dir() or location.is_symlink() or not _SYMBOL.fullmatch(location.name.upper()):
            raise QlibSnapshotError("Unexpected Qlib security directory")
        fields, arrays = {}, {}
        for field in AUDIT_FIELDS:
            path = location / f"{field}.day.bin"
            if not path.exists():
                continue
            values = read_qlib_field(path, days)
            arrays[field] = values
            fields[field] = _field_audit(values, days, field)
            all_fields.add(field)
        complete = np.ones(len(days), dtype=bool)
        for field in REQUIRED_FIELDS:
            values = arrays.get(field)
            if values is None:
                complete[:] = False
            else:
                complete &= np.isfinite(values) & (values > 0 if field != "volume" else values >= 0)
        close = arrays.get("close", np.full(len(days), np.nan))
        issues = [f"missing_field:{field}" for field in AUDIT_FIELDS if field not in fields]
        issues += [f"invalid_values:{field}" for field, info in fields.items() if info["invalid_finite_count"]]
        if all(field in arrays for field in ("open", "high", "low", "close")):
            op, hi, lo, cl = [arrays[field] for field in ("open", "high", "low", "close")]
            bounded = np.isfinite(op) & np.isfinite(hi) & np.isfinite(lo) & np.isfinite(cl)
            inconsistent = bounded & ((hi < np.maximum(op, cl) * (1 - 1e-6))
                | (lo > np.minimum(op, cl) * (1 + 1e-6)) | (hi < lo))
            if inconsistent.any():
                issues.append(f"inconsistent_ohlc:{int(inconsistent.sum())}")
                complete &= ~inconsistent
        valid_positions = np.flatnonzero(complete)
        instruments[location.name.upper()] = {"fields": fields,
            "complete_bar_count": len(valid_positions),
            "first_complete_bar_date": days[valid_positions[0]] if len(valid_positions) else None,
            "last_complete_bar_date": days[valid_positions[-1]] if len(valid_positions) else None,
            "close_missing_at_calendar_end": not bool(np.isfinite(close[-1])),
            "quality_issues": issues}
    if not instruments or not interval_files:
        raise QlibSnapshotError("Qlib dataset has no security bars or membership intervals")
    tail_counts = {}
    for row in instruments.values():
        tail = row["last_complete_bar_date"] or "no_complete_bar"
        tail_counts[tail] = tail_counts.get(tail, 0) + 1
    return {"calendar": {"start": days[0], "end": days[-1], "session_count": len(days),
        "future_start": future[len(days)] if len(future) > len(days) else None,
        "future_end": future[-1] if len(future) > len(days) else None},
        "instrument_files": interval_files, "instruments": instruments,
        "instrument_count": len(instruments), "available_fields": sorted(all_fields),
        "complete_bar_tail_counts": dict(sorted(tail_counts.items())),
        "securities_with_quality_issues": sum(bool(row["quality_issues"]) for row in instruments.values()),
        "calendar_tail_is_not_universal_bar_coverage": True}


@dataclass(frozen=True)
class QlibSnapshot:
    path: Path
    dataset_dir: Path
    manifest: dict
    manifest_sha256: str


def load_qlib_snapshot(path, *, verify=True):
    root = Path(path)
    if root.name == "manifest.json":
        root = root.parent
    if root.is_symlink():
        raise QlibSnapshotError("Snapshot root must not be a symlink")
    root = root.resolve()
    raw = (root / "manifest.json").read_bytes()
    manifest = json.loads(raw)
    kind = manifest.get("kind")
    if (manifest.get("schema_version") != 1 or kind not in {"qlib_cn_daily_snapshot", "qlib_cn_incremental_snapshot"}
            or not _SHA.fullmatch(str(manifest.get("snapshot_id", "")))
            or manifest.get("manifest_sha256") != _digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})):
        raise QlibSnapshotError("Qlib snapshot manifest identity/checksum mismatch")
    if kind == "qlib_cn_daily_snapshot":
        if manifest["snapshot_id"] != manifest.get("archive", {}).get("sha256"):
            raise QlibSnapshotError("Qlib archive snapshot identity mismatch")
    else:
        try:
            lineage = manifest["lineage"]
            parent, increment = lineage["parent_snapshot"], lineage["increment"]
            if (manifest["snapshot_id"] != _increment_identity(manifest)
                    or any(not _SHA.fullmatch(str(parent.get(key, ""))) for key in ("snapshot_id", "manifest_sha256"))
                    or increment.get("filename") != "incremental_manifest.json"
                    or not _SHA.fullmatch(str(increment.get("version_sha256", "")))
                    or manifest["files"]["incremental_manifest.json"]["sha256"] != increment["sha256"]):
                raise QlibSnapshotError("Qlib incremental snapshot identity/lineage mismatch")
        except (KeyError, TypeError):
            raise QlibSnapshotError("Qlib incremental snapshot lineage is incomplete") from None
    dataset = root / "dataset"
    if not dataset.is_dir() or dataset.is_symlink():
        raise QlibSnapshotError("Qlib snapshot dataset is missing or unsafe")
    if verify:
        files = manifest.get("files", {})
        if any(p.is_symlink() for p in dataset.rglob("*")):
            raise QlibSnapshotError("Qlib snapshot must not contain symlinks")
        actual = {str(p.relative_to(dataset)) for p in dataset.rglob("*") if p.is_file()}
        if actual != set(files):
            raise QlibSnapshotError("Qlib snapshot file inventory changed")
        for name, info in files.items():
            relative = _relative(name)
            file = dataset.joinpath(*relative.parts)
            if (file.is_symlink() or any(part.is_symlink() for part in file.parents if part != root.parent)
                    or file.stat().st_size != info["size_bytes"] or _sha(file) != info["sha256"]):
                raise QlibSnapshotError("Qlib snapshot file checksum mismatch")
        if kind == "qlib_cn_incremental_snapshot":
            bridge = json.loads((dataset / "incremental_manifest.json").read_bytes())
            unsigned = {k: v for k, v in bridge.items() if k != "version_sha256"}
            version = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            if (version != bridge.get("version_sha256") or version != increment["version_sha256"]
                    or bridge.get("source") != increment.get("source")
                    or bridge.get("generation") != increment.get("generation")
                    or bridge.get("status") != increment.get("status", "bridge_verified")
                    or bridge.get("status") not in {"bridge_verified", "whole_security_source_rebuild"}
                    or bridge.get("status") == "whole_security_source_rebuild" and bridge.get("mode") != "whole_security_source_rebuild"):
                raise QlibSnapshotError("Qlib incremental source lineage no longer matches its bridge")
    return QlibSnapshot(root, dataset, manifest, hashlib.sha256(raw).hexdigest())


def _increment_identity(manifest):
    return _digest({"kind": "qlib_cn_incremental_snapshot", "files": manifest["files"],
                    "parent_snapshot": manifest["lineage"]["parent_snapshot"],
                    "increment_sha256": manifest["lineage"]["increment"]["sha256"]})


def _inventory(dataset):
    if dataset.is_symlink() or any(p.is_symlink() for p in dataset.rglob("*")):
        raise QlibSnapshotError("Qlib datasets must not contain symlinks")
    return {str(p.relative_to(dataset)): {"sha256": _sha(p), "size_bytes": p.stat().st_size}
            for p in sorted(dataset.rglob("*")) if p.is_file()}


def prepare_qlib_directory(dataset_dir, output_dir, *, source, parent_snapshot, increment_manifest_path):
    """Freeze a bridge output as a distinct directory version, never an archive.

    The parent and every bridge output byte are checked before copying. A
    complete audit and explicit source lineage accompany the new identity;
    updating a subset never advances the untouched securities by association.
    """
    parent_path = parent_snapshot.path if isinstance(parent_snapshot, QlibSnapshot) else parent_snapshot
    parent = load_qlib_snapshot(parent_path)
    output = guard_qlib_output_path(output_dir)
    if output == parent.path or parent.path in output.parents:
        raise QlibSnapshotError("Snapshot output must be outside its immutable parent")
    incoming = Path(dataset_dir)
    increment_path = Path(increment_manifest_path)
    if incoming.is_symlink() or increment_path.is_symlink():
        raise QlibSnapshotError("Increment directories/manifests must not be symlinks")
    incoming, increment_path = incoming.resolve(), increment_path.resolve()
    if incoming == parent.dataset_dir or increment_path != incoming / "incremental_manifest.json":
        raise QlibSnapshotError("A separate bridge dataset and its own incremental manifest are required")
    raw = increment_path.read_bytes()
    increment = json.loads(raw)
    unsigned = {k: v for k, v in increment.items() if k != "version_sha256"}
    version_sha = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    if (increment.get("schema_version") != 1 or increment.get("status") not in {"bridge_verified", "whole_security_source_rebuild"}
            or increment.get("version_sha256") != version_sha or increment.get("base_unchanged") is not True
            or Path(increment.get("base_dir", ".")).resolve() != parent.dataset_dir):
        raise QlibSnapshotError("Bridge manifest checksum/status/parent binding is invalid")
    if increment.get("status") == "whole_security_source_rebuild" and (
            increment.get("mode") != "whole_security_source_rebuild" or not increment.get("selection", {}).get("failure_audit_sha256")):
        raise QlibSnapshotError("Whole-security source replacement lacks its explicit mode/selection evidence")
    files = _inventory(incoming)
    actual = {name: record["sha256"] for name, record in files.items() if name != "incremental_manifest.json"}
    if actual != increment.get("dataset_files"):
        raise QlibSnapshotError("Bridge output file checksum/inventory mismatch")
    if not isinstance(source, dict) or source.get("kind") not in {"official", "community"}:
        raise QlibSnapshotError("Explicit original base-source metadata is required")
    if source != parent.manifest["source"]:
        raise QlibSnapshotError("Incremental base-source metadata differs from the verified parent")
    lineage = {"parent_snapshot": {"snapshot_id": parent.manifest["snapshot_id"],
                   "manifest_sha256": parent.manifest_sha256},
               "increment": {"filename": "incremental_manifest.json", "sha256": hashlib.sha256(raw).hexdigest(),
                   "version_sha256": version_sha, "source": increment.get("source"),
                   "status": increment.get("status"), "mode": increment.get("mode", "strict_incremental_seam"),
                   "generation": increment.get("generation"), "source_evidence": increment.get("source_evidence", {}),
                   "scope": increment.get("scope"), "not_a_full_market_update": increment.get("not_a_full_market_update"),
                   "audited_common_finite_end": increment.get("audited_common_finite_end")}}
    manifest = {"schema_version": 1, "kind": "qlib_cn_incremental_snapshot", "source": source,
                "files": files, "lineage": lineage, "execution_ready": False,
                "price_semantics": parent.manifest["price_semantics"],
                "limitations": list(dict.fromkeys([*parent.manifest["limitations"], *increment.get("limitations", []),
                    "This directory snapshot combines the declared parent with a separately audited incremental collection; it is not a single supplier archive vintage."]))}
    identity = _increment_identity(manifest)
    manifest["snapshot_id"] = identity
    if output == incoming or incoming in output.parents:
        raise QlibSnapshotError("Snapshot output must be outside the bridge input directory")
    with _lock(output):
        destination = output / "snapshots" / identity
        if destination.exists():
            current = load_qlib_snapshot(destination)
            if current.manifest["source"] != source:
                raise QlibSnapshotError("Existing directory snapshot has different source metadata")
            return current
        if shutil.disk_usage(output).free < sum(info["size_bytes"] for info in files.values()) * 2:
            raise QlibSnapshotError("Insufficient disk budget for immutable directory snapshot")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".qlib-increment-", dir=destination.parent) as temporary:
            stage = Path(temporary)
            dataset = stage / "dataset"
            shutil.copytree(incoming, dataset)
            if _inventory(dataset) != files or _inventory(incoming) != files:
                raise QlibSnapshotError("Bridge output changed during immutable publication")
            # Reverify the bound parent as well, rather than trusting a stale in-memory receipt.
            if load_qlib_snapshot(parent.path).manifest_sha256 != parent.manifest_sha256:
                raise QlibSnapshotError("Parent snapshot changed during incremental publication")
            manifest["audit"] = audit_qlib_directory(dataset)
            manifest["created_at"] = datetime.now(timezone.utc).isoformat()
            manifest["manifest_sha256"] = _digest(manifest)
            _write(stage / "manifest.json", manifest)
            stage.rename(destination)
        return load_qlib_snapshot(destination)


def prepare_qlib_snapshot(archive_path, output_dir, *, source, expected_sha256,
                          expected_size_bytes=None, max_extract_bytes=16 * 2 ** 30,
                          max_members=200_000):
    """Verify, safely extract and publish a content-addressed local base.

    ``source`` is explicit provenance metadata, including kind=community or
    official and the immutable release URL/tag. The original archive remains
    untouched. A failed audit never publishes a loadable snapshot.
    """
    output = guard_qlib_output_path(output_dir)
    archive = Path(archive_path).resolve()
    expected_sha256 = str(expected_sha256).removeprefix("sha256:")
    if not _SHA.fullmatch(expected_sha256) or not isinstance(source, dict) or source.get("kind") not in {"official", "community"}:
        raise QlibSnapshotError("Explicit source classification and archive SHA-256 are required")
    if expected_size_bytes is not None and archive.stat().st_size != expected_size_bytes:
        raise QlibSnapshotError("Archive size differs from source manifest")
    declaration = source.get("release_manifest", {})
    if (declaration.get("archive_sha256") and str(declaration["archive_sha256"]).removeprefix("sha256:") != expected_sha256
            or declaration.get("archive_size_bytes") and declaration["archive_size_bytes"] != archive.stat().st_size
            or declaration.get("release_tag") and declaration["release_tag"] != source.get("release_tag")):
        raise QlibSnapshotError("Pinned source declaration contradicts archive identity")
    if _sha(archive) != expected_sha256:
        raise QlibSnapshotError("Archive SHA-256 differs from source manifest")
    with _lock(output):
        destination = output / "snapshots" / expected_sha256
        if destination.exists():
            current = load_qlib_snapshot(destination)
            if current.manifest["source"] != source:
                raise QlibSnapshotError("The same archive already has different source metadata")
            return current
        if shutil.disk_usage(output).free < max_extract_bytes + archive.stat().st_size:
            raise QlibSnapshotError("Insufficient disk budget for bounded extraction")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".qlib-", dir=destination.parent) as temporary:
            stage = Path(temporary)
            extracted = stage / "unpacked"
            extracted.mkdir()
            expanded_bytes = _extract(archive, extracted, max_extract_bytes, max_members)
            roots = [p.parent.parent for p in extracted.rglob("calendars/day.txt")
                     if (p.parent.parent / "features").is_dir() and (p.parent.parent / "instruments").is_dir()]
            if len(roots) != 1:
                raise QlibSnapshotError("Archive must contain exactly one Qlib daily dataset root")
            dataset = stage / "dataset"
            roots[0].rename(dataset)
            if extracted.exists():
                shutil.rmtree(extracted)
            audit = audit_qlib_directory(dataset)
            declared = source.get("release_manifest", {})
            if declared.get("target_trade_date") and audit["calendar"]["end"] != declared["target_trade_date"]:
                raise QlibSnapshotError("Historical calendar contradicts the release manifest")
            for key, actual_key in (("future_start_date", "future_start"), ("future_end_date", "future_end")):
                if declared.get(key) and declared[key] != audit["calendar"][actual_key]:
                    raise QlibSnapshotError("Future calendar contradicts the release manifest")
            files = {str(p.relative_to(dataset)): {"sha256": _sha(p), "size_bytes": p.stat().st_size}
                     for p in sorted(dataset.rglob("*")) if p.is_file()}
            manifest = {"schema_version": 1, "kind": "qlib_cn_daily_snapshot",
                "snapshot_id": expected_sha256, "created_at": datetime.now(timezone.utc).isoformat(),
                "source": source, "archive": {"filename": archive.name, "sha256": expected_sha256,
                    "size_bytes": archive.stat().st_size, "expanded_bytes": expanded_bytes,
                    "original_archive_retained": True}, "files": files, "audit": audit,
                "execution_ready": False, "price_semantics": {
                    "adjusted_prices": "supplier Qlib-normalized OHLC/VWAP; not integer-share execution prices",
                    "factor": "supplier field; normalization and raw-price reconstruction require source validation",
                    "volume_unit_verified": False, "corporate_actions_complete": False,
                    "point_in_time_adjustment_vintage_verified": False},
                "limitations": [
                    "A community archive recommended by Qlib is not a Microsoft official market-data release.",
                    "Each security/field has its own actual finite-bar tail; calendar and release dates are not universal coverage.",
                    "Historical membership intervals record effective ranges; announcement/knowledge times are unverified.",
                    "Repository code licensing does not independently establish underlying market-data redistribution rights.",
                    "Do not directly concatenate another provider: raw price, factor normalization, volume units and overlap require validation.",
                    "No missing observations are filled and no future calendar session is treated as an observed bar."]}
            manifest["manifest_sha256"] = _digest(manifest)
            _write(stage / "manifest.json", manifest)
            stage.rename(destination)
        return load_qlib_snapshot(destination)


def download_qlib_archive(url, archive_path, *, expected_sha256, expected_size_bytes,
                          max_archive_bytes=2 * 2 ** 30, session=None, timeout=45):
    """Stream exactly one pinned HTTPS archive; preserve verified original bytes.

    No authentication, host fallback, retry loop or external executable is
    involved. Existing bytes are reused only after complete size/SHA validation.
    """
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise QlibSnapshotError("An explicit public HTTPS archive URL is required")
    expected_sha256 = str(expected_sha256).removeprefix("sha256:")
    if not _SHA.fullmatch(expected_sha256) or not 0 < expected_size_bytes <= max_archive_bytes:
        raise QlibSnapshotError("Require archive SHA-256 and size within the download budget")
    target = Path(archive_path).resolve()
    guard_qlib_output_path(target.parent)
    target.parent.mkdir(parents=True, exist_ok=True)
    with _lock(target.parent):
        if target.exists():
            if target.stat().st_size != expected_size_bytes or _sha(target) != expected_sha256:
                raise QlibSnapshotError("Existing archive does not match its pinned source")
            return target
        if shutil.disk_usage(target.parent).free < expected_size_bytes * 2:
            raise QlibSnapshotError("Insufficient disk budget for archive download")
        import requests
        client = session or requests.Session()
        temporary = target.with_name(target.name + ".part")
        sha, received = hashlib.sha256(), 0
        try:
            with client.get(url, stream=True, timeout=(10, timeout)) as response:
                if response.status_code != 200:
                    raise QlibSnapshotError(f"Archive HTTP status {response.status_code}; no fallback attempted")
                declared = response.headers.get("Content-Length")
                if declared is not None and int(declared) != expected_size_bytes:
                    raise QlibSnapshotError("Archive response size differs from the pinned source")
                with temporary.open("wb") as output:
                    for chunk in response.iter_content(2 ** 20):
                        if not chunk:
                            continue
                        received += len(chunk)
                        if received > expected_size_bytes:
                            raise QlibSnapshotError("Archive response exceeds the pinned size")
                        sha.update(chunk)
                        output.write(chunk)
            if received != expected_size_bytes or sha.hexdigest() != expected_sha256:
                raise QlibSnapshotError("Downloaded archive does not match its pinned checksum/size")
            temporary.replace(target)
            return target
        finally:
            if session is None:
                client.close()
