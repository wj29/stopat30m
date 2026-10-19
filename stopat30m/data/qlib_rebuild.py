"""Whole-security source replacement after a strict cross-source seam failed.

This is deliberately not an incremental seam. Every selected security's
history is replaced by one declared raw/HFQ collection; unrelated securities
and historical index intervals retain their parent bytes. No model is used.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

import numpy as np
import pandas as pd

from .ashare_daily import load_collection_frames
from .historical_universe import normalize_member
from .qlib_incremental import BridgePolicy, CanonicalIncrement, read_qlib_security
from .qlib_snapshot import _inventory, _lock, guard_qlib_output_path
from .research_calendar import Calendar, iso_day

FIELDS = ("open", "high", "low", "close", "volume", "factor", "vwap")
OHLC = ("open", "high", "low", "close")
MODE = "whole_security_source_rebuild"


class RebuildError(ValueError):
    pass


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _validated_subset_receipt(root, collection):
    """A subset is a view of original bytes, never a new acquisition vintage."""
    declared = collection.get("derived_view")
    if declared is None:
        return None
    path = root / "subset_receipt.json"
    raw = path.read_bytes()
    receipt = json.loads(raw)
    if receipt != declared or receipt.get("kind") != "validated_source_subset_view":
        raise RebuildError("Subset receipt differs from the collection declaration")
    original_root = Path(receipt["original_collection"])
    original_raw = (original_root / "collection.json").read_bytes()
    original = json.loads(original_raw)
    selected = set(collection["instruments"])
    excluded = receipt.get("excluded_instruments", {})
    if (_sha(original_raw) != receipt.get("original_collection_manifest_sha256")
            or original["generation"] != collection["generation"]
            or collection["generation"] != receipt.get("original_generation_preserved")
            or set(receipt.get("selected_instruments", [])) != selected
            or selected & set(excluded) or selected | set(excluded) != set(original["instruments"])
            or receipt.get("new_source_generation_claimed") is not False
            or receipt.get("raw_response_bytes_unmodified") is not True
            or receipt.get("original_response_fetched_at_preserved") is not True):
        raise RebuildError("Subset does not bind the original selection and acquisition generation")
    for key, record in collection["responses"].items():
        if original["responses"].get(key) != record or Path(record["file"]).name != record["file"]:
            raise RebuildError("Subset response identity/retrieval time differs from its original")
        copied = (root / record["file"]).read_bytes()
        source = (original_root / record["file"]).read_bytes()
        if copied != source or _sha(source) != record["sha256"]:
            raise RebuildError("Subset response bytes differ from their immutable originals")
    return {"subset_receipt_sha256": _sha(raw),
            "original_collection_manifest_sha256": _sha(original_raw),
            "original_generation_preserved": original["generation"],
            "selected_instruments": sorted(selected), "excluded_instruments": excluded}


def rebuild_batch_from_collection(collection_dir, base_dir, calendar_path):
    """Use packaged historical sessions plus a real, crosschecked newer calendar."""
    raw, adjusted, original = load_collection_frames(collection_dir)
    subset = _validated_subset_receipt(Path(collection_dir), original)
    evidence = {"original_collection": str(Path(collection_dir).resolve()),
        "collection_manifest_sha256": _sha((Path(collection_dir) / "collection.json").read_bytes()),
        "responses": original["responses"],
        **({"validated_subset_view": subset} if subset else {})}
    return _rebuild_batch(raw, adjusted, original, base_dir, calendar_path,
                          generation=original["generation"], evidence=evidence)


def rebuild_batch_from_sources(original_collection, base_dir, calendar_path, *,
                               replacements, factor_repairs, receipt_dir):
    """A locally composed evidence set, never an atomic supplier generation."""
    from .ashare_repair import compose_rebuild_sources
    raw, adjusted, receipt = compose_rebuild_sources(original_collection,
        replacements=replacements, factor_repairs=factor_repairs, receipt_dir=receipt_dir)
    return _rebuild_batch(raw, adjusted, receipt, base_dir, calendar_path,
        generation="local-composition:" + receipt["local_composition_id"],
        evidence={"source_composition": receipt, "atomic_supplier_vintage_verified": False,
                  "supplier_generation_claimed": False})


def _rebuild_batch(raw, adjusted, original, base_dir, calendar_path, *, generation, evidence):
    base = Path(base_dir)
    old_days = (base / "calendars/day.txt").read_text().splitlines()
    if not old_days or old_days != sorted(set(old_days)):
        raise RebuildError("Historical base calendar is invalid")
    new = Calendar.load(calendar_path)
    new._require_coverage(original["request_end"])
    old_set = set(old_days)
    for day, flag in new.days:
        if old_days[0] <= day <= old_days[-1] and bool(flag) != (day in old_set):
            raise RebuildError("Historical and newer source calendars disagree")
    start, end = original["request_start"], original["request_end"]
    if start != old_days[0] or end < old_days[-1]:
        raise RebuildError("Whole-security request must cover the complete parent calendar")
    newer_flags = dict(new.days)
    rows = []
    for stamp in pd.date_range(start, end):
        day = stamp.date().isoformat()
        if day <= old_days[-1]:
            flag = day in old_set
        elif day in newer_flags:
            flag = bool(newer_flags[day])
        else:
            raise RebuildError("A real source calendar is missing for the rebuild tail")
        rows.append({"date": day, "is_open": int(flag)})
    source = "packaged_qlib_exchange_sessions+" + new.source
    url = "https://github.com/chenditc/investment_data/ ; " + new.source_url
    return CanonicalIncrement.from_frames(raw, adjusted, source="baostock", generation=generation,
        calendar_rows=pd.DataFrame(rows), calendar_source=source, calendar_source_url=url,
        requested_start=start, requested_end=end,
        evidence={**evidence, "source_request": {"start_date": start, "end_date": end},
            "historical_calendar_sha256": _sha((base / "calendars/day.txt").read_bytes()),
            "newer_calendar_sha256": _sha(Path(calendar_path).read_bytes()),
            "source_volume_unit": "shares", "source_amount_unit": "CNY"})


def _selection(path, instruments):
    raw = Path(path).read_bytes()
    audit = json.loads(raw)
    failed = audit.get("failed_instruments")
    if not isinstance(failed, list) or len(failed) != len(set(failed)) or set(failed) != set(instruments):
        raise RebuildError("Whole-security replacement must match exactly the failed seam audit selection")
    return {"failure_audit_sha256": _sha(raw), "failure_audit_file": Path(path).name,
            "failed_instruments": sorted(failed), "selection_basis": "failed_predeclared_strict_data_seam_not_returns"}


def _normalized(base, batch, failure_audit_path):
    if batch.source != "baostock" or not batch.generation or batch.volume_unit != "shares" or not batch.evidence:
        raise RebuildError("Require explicit BaoStock raw/HFQ source evidence and share-volume units")
    frame = batch.bars.copy()
    required = {"date", "instrument", "raw_volume", "raw_amount", "tradestatus",
                *[f"{mode}_{field}" for mode in ("raw", "adjusted") for field in OHLC]}
    if not required.issubset(frame) or frame.empty:
        raise RebuildError("Rebuild requires complete raw/HFQ fields, amount and source trading flags")
    frame["date"] = frame.date.map(iso_day)
    frame["instrument"] = frame.instrument.map(normalize_member)
    if frame.duplicated(["date", "instrument"]).any():
        raise RebuildError("Duplicate source security/session rows")
    instruments = sorted(frame.instrument.unique())
    selection = _selection(failure_audit_path, instruments)
    calendar = Calendar.from_rows(batch.calendar_rows, batch.requested_start, batch.requested_end,
        source=batch.calendar_source, source_url=batch.calendar_source_url)
    sessions = calendar.sessions(batch.requested_start, batch.requested_end)
    old = (base / "calendars/day.txt").read_text().splitlines()
    if batch.requested_start != old[0] or batch.requested_end < old[-1] or sessions[:len(old)] != old:
        raise RebuildError("Rebuild must cover and retain the parent's entire real exchange calendar")
    if not frame.date.isin(sessions).all():
        raise RebuildError("Source rebuild bars occur outside the verified exchange calendar")
    numeric = sorted(required - {"date", "instrument", "tradestatus"})
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="raise").astype(float)
    if np.isinf(frame[numeric].to_numpy()).any() or frame[numeric].lt(0).any().any():
        raise RebuildError("Source history contains negative or infinite prices/units")
    status = frame.tradestatus.astype(str)
    if not status.isin({"0", "1"}).all():
        raise RebuildError("Unknown source trading flags cannot be classified as executable or halted")
    policy = BridgePolicy(base_raw_volume_multiplier=100)
    outputs, reports, statuses = {}, {}, {}
    for inst, group in frame.groupby("instrument", sort=True):
        group = group.sort_values("date").set_index("date")
        trading = group.tradestatus.astype(str).eq("1")
        finite = np.isfinite(group[numeric]).all(axis=1)
        positive = group[numeric].gt(0).all(axis=1)
        available = trading & finite & positive
        # Blank/zero quotation fields remain explicitly unavailable, not cash
        # returns or guessed suspensions. Negative/infinite values failed above.
        if not available.any():
            raise RebuildError(f"No complete, finite traded source history for {inst}")
        checked = group.loc[available]
        if not np.isclose(checked.raw_volume, np.rint(checked.raw_volume), rtol=0, atol=1e-6).all():
            raise RebuildError(f"Raw share-volume is not integer-valued for {inst}")
        for prefix in ("raw", "adjusted"):
            upper, lower = checked[f"{prefix}_high"], checked[f"{prefix}_low"]
            if (upper.lt(checked[[f"{prefix}_{field}" for field in OHLC]].max(axis=1) - 1e-6)
                    | lower.gt(checked[[f"{prefix}_{field}" for field in OHLC]].min(axis=1) + 1e-6)).any():
                raise RebuildError(f"Source OHLC range is inconsistent for {inst}")
        factor = checked.adjusted_close / checked.raw_close
        factor_errors = {}
        for field in OHLC:
            expected = checked[f"raw_{field}"] * factor
            actual = checked[f"adjusted_{field}"]
            if not np.isclose(actual, expected, rtol=policy.factor_rtol, atol=1e-6).all():
                raise RebuildError(f"Raw/HFQ is not one multiplicative OHLC factor for {inst}/{field}")
            factor_errors[field] = float(((actual - expected).abs() / expected.abs()).max())
        raw_vwap = checked.raw_amount / checked.raw_volume
        tolerance = checked.raw_high.abs() * policy.price_rtol + 1e-6
        if ((raw_vwap < checked.raw_low - tolerance) | (raw_vwap > checked.raw_high + tolerance)).any():
            raise RebuildError(f"Raw CNY amount/share-volume VWAP is outside source OHLC for {inst}")
        normalization = float(checked.adjusted_close.iloc[0])
        qfactor = factor / normalization
        output = pd.DataFrame(np.nan, index=pd.Index(sessions, name="date"), columns=FIELDS)
        for field in OHLC:
            output.loc[checked.index, field] = checked[f"adjusted_{field}"] / normalization
        output.loc[checked.index, "factor"] = qfactor
        output.loc[checked.index, "volume"] = checked.raw_volume / (100 * qfactor)
        output.loc[checked.index, "vwap"] = raw_vwap * qfactor
        before = read_qlib_security(base, inst)
        before_valid = before.notna().all(axis=1) & before.gt(0).all(axis=1)
        first = checked.index[0]
        if before_valid.any() and first > before.index[before_valid][0]:
            raise RebuildError(f"New source loses the parent's earlier finite history for {inst}")
        if before_valid.any() and checked.index[-1] < before.index[before_valid][-1]:
            raise RebuildError(f"New source loses the parent's newer finite history for {inst}")
        source_status = {}
        for day in sessions:
            if group.index[0] <= day <= group.index[-1]:
                source_status[day] = ("source_missing_bar" if day not in group.index
                    else "source_suspended" if str(group.at[day, "tradestatus"]) == "0"
                    else "available" if bool(available.at[day]) else "source_incomplete_quote")
        statuses[inst] = source_status
        old_fields = sorted(path.name for path in (base / "features" / inst.lower()).glob("*.day.bin"))
        parent_finite = before.index[before_valid].tolist()
        now_unavailable = [day for day in parent_finite if not output.loc[day].notna().all()]
        outputs[inst] = output.loc[group.index[0]:group.index[-1]]
        reports[inst] = {"mode": MODE, "source_first_observed_date": group.index[0],
            "source_last_observed_date": group.index[-1], "first_complete_finite": first,
            "last_complete_finite": checked.index[-1], "source_rows": len(group),
            "finite_trading_rows": len(checked), "normalization_first_adjusted_close": normalization,
            "max_same_source_factor_relative_errors": factor_errors,
            "suspended_dates": [day for day, value in source_status.items() if value == "source_suspended"],
            "unknown_missing_dates": [day for day, value in source_status.items() if value in {"source_missing_bar", "source_incomplete_quote"}],
            "parent_finite_session_count": len(parent_finite),
            "parent_finite_sessions_now_unavailable": now_unavailable,
            "parent_finite_sessions_now_unavailable_count": len(now_unavailable),
            "parent_finite_gap_source_status": {day: source_status.get(day, "source_missing_bar") for day in now_unavailable},
            "whole_history_equivalent_to_parent_claimed": False,
            "deleted_extra_features": [name for name in old_fields if name not in {field + ".day.bin" for field in FIELDS}],
            "replaced_features": list(FIELDS), "parent_feature_sha256": before.attrs["checksums"]}
        composition = batch.evidence.get("source_composition")
        if composition:
            reports[inst]["source_composition_kind"] = composition["securities"][inst]["kind"]
            reports[inst]["source_provenance"] = composition["securities"][inst]
    return outputs, reports, statuses, sessions, selection, calendar


def publish_whole_security_rebuild(base_dir, batch, output_dir, *, failure_audit_path):
    """Clone the parent and replace exactly the failed securities' full history."""
    base, target = Path(base_dir).resolve(), Path(output_dir).resolve()
    if base == target or base in target.parents or target in base.parents or target.exists():
        raise RebuildError("Whole-security rebuild needs a new directory outside its immutable parent")
    guard_qlib_output_path(target)
    if any(path.is_symlink() for path in base.rglob("*")):
        raise RebuildError("Parent dataset contains unsafe symlinks")
    outputs, reports, statuses, sessions, selection, calendar = _normalized(base, batch, failure_audit_path)
    parent_files = _inventory(base)
    with _lock(target.parent):
        if target.exists():
            raise FileExistsError("Whole-security rebuild directory already exists")
        with tempfile.TemporaryDirectory(prefix=".whole-security-", dir=target.parent) as temporary:
            stage = Path(temporary) / "dataset"
            shutil.copytree(base, stage)
            (stage / "calendars/day.txt").write_text("\n".join(sessions) + "\n")
            for inst, output in outputs.items():
                directory = stage / "features" / inst.lower()
                for old in directory.glob("*.day.bin"):
                    old.unlink()
                offset = sessions.index(output.index[0])
                for field in FIELDS:
                    values = output[field].to_numpy(dtype="<f4")
                    if np.isinf(values).any():
                        raise RebuildError("Normalized source values overflow Qlib float32 storage")
                    (directory / (field + ".day.bin")).write_bytes(
                        np.asarray([offset], dtype="<f4").tobytes() + values.tobytes())
            status_path = stage / "rebuild_status.json"
            combined_status = json.loads(status_path.read_text()) if status_path.exists() else {}
            combined_status.update(statuses)
            status_path.write_text(json.dumps(combined_status, ensure_ascii=False, sort_keys=True) + "\n")
            # All historical index intervals remain byte-identical. The all
            # availability file may extend only the selected source rows.
            all_path = stage / "instruments/all.txt"
            rows = [line.split("\t") for line in all_path.read_text().splitlines() if line.strip()]
            for row in rows:
                report = reports.get(row[0].upper())
                if report:
                    row[2] = max(row[2], report["source_last_observed_date"])
            all_path.write_text("".join("\t".join(row) + "\n" for row in rows))
            manifest = {"schema_version": 1, "status": MODE, "mode": MODE,
                "base_dir": str(base), "base_unchanged": True, "source": batch.source,
                "generation": batch.generation, "source_evidence": batch.evidence,
                "atomic_supplier_vintage_verified": False,
                "created_at": datetime.now(timezone.utc).isoformat(), "selection": selection,
                "scope": "base_clone_with_whole_security_source_replacements",
                "not_a_full_market_update": True, "securities": reports,
                "source_bars_sha256": _sha(batch.bars.to_csv(index=False, float_format="%.17g").encode()),
                "policy": {"normalization": "first_complete_source_adjusted_close=1",
                    "base_volume_unit": "100_share_lots_divided_by_qfactor", "raw_volume_unit": "shares",
                    "raw_amount_unit": "CNY", "same_source_factor_rtol": .0005,
                    "source_vwap_ohlc_rtol": .0005, "cross_source_seam_claimed": False},
                "calendar_provenance": {"source": calendar.source, "source_url": calendar.source_url},
                "audited_common_finite_end": min(report["last_complete_finite"] for report in reports.values()),
                "execution_ready": False,
                "limitations": ["Selected full histories now use BaoStock, not the parent's adjusted-price vintage; old comparison results remain tied to their old snapshot.",
                    "The replacement was selected by a failed strict data seam, not model performance; no seam tolerance was relaxed.",
                    "Suspended/missing quotes remain NaN with explicit source status; missing dates are not invented holidays or zero returns.",
                    "Other securities and historical index intervals retain their parent coverage; this is not a full-market refresh."]}
            # Never retain a parent bridge manifest as this child's source claim.
            (stage / "incremental_manifest.json").unlink(missing_ok=True)
            manifest["dataset_files"] = {name: info["sha256"] for name, info in _inventory(stage).items()}
            manifest["version_sha256"] = _sha(_json_bytes(manifest))
            (stage / "incremental_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            if _inventory(base) != parent_files:
                raise RebuildError("Immutable parent changed during whole-security publication")
            # No write can occur outside this staged tree before all checks pass.
            stage.rename(target)
    return manifest
