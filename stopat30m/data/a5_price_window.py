"""Uniform, source-bound research windows; no patching individual bad dates.

The retained binary observations are copied byte for byte with a new calendar
offset. Original responses, older histories and parent snapshots remain intact.
The existing directory-snapshot publisher records this exact source bridge as a
local date/scope view, never as a new atomic supplier acquisition.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
from uuid import uuid4

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import REQUIRED_FIELDS
from .ashare_daily import load_collection_frames
from .ashare_scope import load_verified_historical_scope
from .qlib_incremental import CanonicalIncrement
from .qlib_snapshot import (
    _inventory, _lock, guard_qlib_output_path, load_qlib_snapshot, prepare_qlib_directory,
)


SPEC_PATH = Path(__file__).parents[2] / "docs/research/a5_price_window_spec.json"


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
        allow_nan=False) + "\n")


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path)}


def clip_field_bytes(raw, *, old_calendar_length, first, last):
    """Copy retained IEEE-754 payload bits, including NaNs, without recasting."""
    if (len(raw) < 8 or len(raw) % 4 or not 0 <= first <= last < old_calendar_length):
        raise ValueError("A valid original field and inclusive actual-session window are required")
    original = np.frombuffer(raw, dtype="<f4")
    offset = float(original[0])
    if (not np.isfinite(offset) or offset < 0 or int(offset) != offset
            or int(offset) + len(original)-1 > old_calendar_length or np.isinf(original[1:]).any()):
        raise ValueError("Malformed original Qlib field layout")
    offset = int(offset)
    left, right = max(offset, first), min(offset+len(original)-1, last+1)
    if left >= right:
        raise ValueError("An official selected security field has no observations in the common price window")
    start, end = 4*(left-offset+1), 4*(right-offset+1)
    payload = raw[start:end]
    result = np.asarray([left-first], dtype="<f4").tobytes() + payload
    if result[4:] != raw[start:end]:
        raise ValueError("Retained original floating-point payload changed")
    return result, {"original_calendar_offset": offset, "new_calendar_offset": left-first,
        "original_observation_count": len(original)-1, "retained_observation_count": right-left,
        "retained_original_byte_range": [start, end],
        "retained_payload_sha256": hashlib.sha256(payload).hexdigest()}


def _window_policy(path):
    value = json.loads(Path(path).read_text())
    if (value.get("kind") != "a5_uniform_model_price_window"
            or value.get("requested_price_start") != "2017-01-01"
            or value.get("end") != "2026-10-19"
            or value.get("first_membership_and_training_year") != 2018
            or value.get("performance_used_to_choose_window") is not False
            or value.get("price_or_factor_interpolation") is not False
            or value.get("drop_bad_rows_inside_declared_window") is not False):
        raise ValueError("The separately declared uniform A5 price-window policy is required")
    return value


def publish_uniform_price_window(parent_snapshot, scope_directory, output_dir, *, policy_path=SPEC_PATH):
    """Preserve a uniform window of every official historical union member."""
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Preserve the previous A5 price-window generation")
    policy = _window_policy(policy_path)
    parent = load_qlib_snapshot(parent_snapshot, verify=True)
    calendar_path = parent.dataset_dir / "calendars/day.txt"
    calendar = calendar_path.read_text().splitlines()
    selected = [day for day in calendar if policy["requested_price_start"] <= day <= policy["end"]]
    if not selected or selected[-1] != policy["end"]:
        raise ValueError("The original actual calendar does not cover the entire common research window")
    first, last = calendar.index(selected[0]), calendar.index(selected[-1])
    scope = load_verified_historical_scope(scope_directory, start="2018-01-01", end=policy["end"], require_official=True)
    with scope.verified_batch():
        symbols = scope.download_targets("2018-01-01", policy["end"], sessions=calendar)
    proofs = {"parent_manifest": _proof(parent.path / "manifest.json"),
        "policy": _proof(policy_path), "implementation": _proof(__file__), **scope.source_receipts()}
    target.parent.mkdir(parents=True, exist_ok=True)
    with _lock(target.parent):
        target.mkdir()
        dataset = target / "dataset_view"
        (dataset / "calendars").mkdir(parents=True)
        (dataset / "instruments").mkdir()
        (dataset / "features").mkdir()
        (dataset / "calendars/day.txt").write_text("\n".join(selected) + "\n")
        original_all = parent.dataset_dir / "instruments/all.txt"
        availability, found = [], set()
        for line in original_all.read_text().splitlines():
            if not line.strip():
                continue
            symbol, left, right = line.split()
            symbol = symbol.upper()
            if symbol in symbols and right >= selected[0] and left <= selected[-1]:
                availability.append(f"{symbol}\t{max(left, selected[0])}\t{min(right, selected[-1])}\n")
                found.add(symbol)
        if found != set(symbols):
            raise ValueError("Parent availability table omits an official selected identity: " + ",".join(sorted(set(symbols)-found)))
        (dataset / "instruments/all.txt").write_text("".join(sorted(availability)))
        for name in ("csi300", "csi1000"):
            shutil.copyfile(Path(scope_directory) / (name + ".txt"), dataset / "instruments" / (name + ".txt"))
        reports = {}
        for symbol in symbols:
            directory = dataset / "features" / symbol.lower()
            directory.mkdir()
            reports[symbol] = {}
            for field in REQUIRED_FIELDS:
                name = f"features/{symbol.lower()}/{field}.day.bin"
                original = parent.dataset_dir / name
                raw = original.read_bytes()
                if hashlib.sha256(raw).hexdigest() != parent.manifest["files"][name]["sha256"]:
                    raise ValueError("Parent source field changed while constructing its exact view")
                clipped, report = clip_field_bytes(raw, old_calendar_length=len(calendar), first=first, last=last)
                (directory / (field + ".day.bin")).write_bytes(clipped)
                reports[symbol][field] = {**report, "original_sha256": hashlib.sha256(raw).hexdigest(),
                    "view_sha256": hashlib.sha256(clipped).hexdigest()}
        # Preserve already evidenced missing/halted classifications, only within
        # this common window. No new classification is inferred by slicing.
        status_path = parent.dataset_dir / "rebuild_status.json"
        if status_path.exists():
            statuses = json.loads(status_path.read_text())
            statuses = {symbol: {day: value for day, value in days.items()
                if selected[0] <= day <= selected[-1]} for symbol, days in statuses.items() if symbol in symbols}
            _save(dataset / "rebuild_status.json", statuses)
        receipt = {"kind": "a5_exact_original_float32_history_window", "created_at": datetime.now(timezone.utc).isoformat(),
            "policy": policy, "actual_first_session": selected[0], "actual_last_session": selected[-1],
            "session_count": len(selected), "instruments": symbols, "fields": list(REQUIRED_FIELDS),
            "source_fields": reports, "bound_inputs": proofs, "price_values_modified": False,
            "partial_or_bad_rows_removed_inside_window": False, "portfolio_returns_computed": False,
            "supplier_acquisition_or_atomic_vintage_claimed": False,
            "market_evidence_audit_passed": False, "execution_ready": False}
        _save(dataset / "price_window_receipt.json", receipt)
        bridge = {"schema_version": 1, "status": "bridge_verified", "mode": "exact_original_payload_date_scope_view",
            "base_unchanged": True, "base_dir": str(parent.dataset_dir), "source": "local_verified_parent_byte_view",
            "generation": "local-view:" + uuid4().hex,
            "source_evidence": {"price_window_receipt": _proof(dataset / "price_window_receipt.json"),
                "meaning_of_bridge_verified": "only_exact_retained_payload_equality_not_market_quality_or_supplier_seam_certification",
                "original_supplier_generation_claimed": False},
            "scope": "uniform_2017_price_window_official_2018_onward_historical_union_targets",
            "not_a_full_market_update": True, "audited_common_finite_end": None,
            "limitations": ["Every selected security uses the same declared calendar window; pre-2017 history remains in the untouched parent.",
                "2017 prices are feature warmup only; verified membership and training begin in 2018.",
                "This exact byte-preserving view has not passed the separate raw-price/status/action market audit."],
            "dataset_files": {name: proof["sha256"] for name, proof in _inventory(dataset).items()}}
        bridge["version_sha256"] = hashlib.sha256(json.dumps(bridge, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        _save(dataset / "incremental_manifest.json", bridge)
    scope.assert_unchanged()
    if any(sha(proof["path"]) != proof["sha256"] for proof in proofs.values()):
        raise ValueError("Uniform window source or implementation changed during publication")
    frozen = prepare_qlib_directory(dataset, target / "versions", source=parent.manifest["source"],
        parent_snapshot=parent, increment_manifest_path=dataset / "incremental_manifest.json")
    _save(target / "publication.json", {"snapshot_path": str(frozen.path), "snapshot_manifest_sha256": frozen.manifest_sha256,
        "source_snapshot_manifest_sha256": parent.manifest_sha256, "window_receipt_sha256": sha(dataset / "price_window_receipt.json"),
        "execution_ready": False, "market_audit_required": True})
    return frozen


def original_collection_window_batch(collection_directory, window_snapshot, *, receipt_path):
    """Use an explicit date view of full original responses, never rewrite them."""
    receipt_path = guard_qlib_output_path(receipt_path)
    if receipt_path.exists():
        raise ValueError("Preserve the original collection-window view receipt")
    snapshot = load_qlib_snapshot(window_snapshot, verify=True)
    receipt = json.loads((snapshot.dataset_dir / "price_window_receipt.json").read_text())
    if (receipt.get("policy") != _window_policy(SPEC_PATH)
            or receipt.get("bound_inputs", {}).get("policy", {}).get("sha256") != sha(SPEC_PATH)):
        raise ValueError("Collection view differs from the previously declared uniform price-window policy")
    raw, adjusted, manifest = load_collection_frames(collection_directory)
    calendar = (snapshot.dataset_dir / "calendars/day.txt").read_text().splitlines()
    start, end = calendar[0], calendar[-1]
    if (manifest["request_start"] > start or manifest["request_end"] < end
            or not set(manifest["instruments"]).issubset(receipt["instruments"])):
        raise ValueError("Original replacement source does not cover the approved date/identity view")
    original_proofs = {"collection_manifest": _proof(Path(collection_directory) / "collection.json"),
        "window_snapshot_manifest": _proof(snapshot.path / "manifest.json"), "implementation": _proof(__file__)}
    for key, proof in manifest["responses"].items():
        path = Path(collection_directory) / proof["file"]
        if sha(path) != proof["sha256"]:
            raise ValueError("Original replacement response changed")
        original_proofs[key] = _proof(path)
    view_raw, view_adjusted = raw.loc[raw.date.between(start, end)].copy(), adjusted.loc[adjusted.date.between(start, end)].copy()
    payload = {"kind": "a5_original_collection_uniform_date_view", "created_at": datetime.now(timezone.utc).isoformat(),
        "original_request": [manifest["request_start"], manifest["request_end"]], "view_request": [start, end],
        "original_generation_preserved": manifest["generation"], "new_supplier_generation_claimed": False,
        "value_or_response_bytes_modified": False, "raw_rows_original": len(raw), "raw_rows_retained": len(view_raw),
        "adjusted_rows_original": len(adjusted), "adjusted_rows_retained": len(view_adjusted),
        "excluded_dates_are_outside_uniform_declared_price_window": True, "bound_inputs": original_proofs,
        "raw_view_sha256": hashlib.sha256(view_raw.to_csv(index=False).encode()).hexdigest(),
        "adjusted_view_sha256": hashlib.sha256(view_adjusted.to_csv(index=False).encode()).hexdigest()}
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    _save(receipt_path, payload)
    actual_sessions = set(calendar)
    rows = [{"date": stamp.date().isoformat(), "is_open": int(stamp.date().isoformat() in actual_sessions)}
        for stamp in pd.date_range(start, end)]
    return CanonicalIncrement.from_frames(view_raw, view_adjusted, source="baostock",
        generation="local-date-view:" + manifest["generation"], calendar_rows=pd.DataFrame(rows),
        calendar_source="byte_bound_parent_actual_exchange_sessions",
        calendar_source_url=snapshot.manifest["source"]["url"], requested_start=start, requested_end=end,
        evidence={"uniform_source_date_view": _proof(receipt_path), "originals": original_proofs,
            "supplier_generation_claimed": False, "atomic_supplier_vintage_verified": False})
