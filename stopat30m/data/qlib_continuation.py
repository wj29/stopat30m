"""Strict incremental seams with immutable previously audited source units.

The original incremental module remains unchanged because historical training
proofs bind its exact bytes. This version reuses its canonical types and checks;
parent continuity does not accept historical source revisions or fit new scales.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
import pandas as pd

from .qlib_incremental import (
    BridgeError, BridgePolicy, CanonicalIncrement, OHLC, CORE_FIELDS,
    SCHEMA_VERSION, REFERENCES, _calendar, _error_stats, _inventory,
    _json_bytes, _read_feature, _sha, _validated_batch, read_qlib_security,
)


@dataclass(frozen=True)
class NormalizationContinuation:
    """Previously published source units, never a fitted fallback after failure.

    The previous source batch must reproduce its immutable increment receipt.
    Its common observations must be unchanged before any inherited scale can
    enter the ordinary seven-field seam checks.
    """
    previous_batch: CanonicalIncrement
    parent_increment_sha256: str

    def __post_init__(self):
        if (not isinstance(self.previous_batch, CanonicalIncrement)
                or not isinstance(self.parent_increment_sha256, str)
                or len(self.parent_increment_sha256) != 64
                or any(c not in "0123456789abcdef" for c in self.parent_increment_sha256)):
            raise BridgeError("Continuation requires an exact published parent increment proof")


def _continuation_scales(base, batch, bars, policy, continuation):
    if not isinstance(continuation, NormalizationContinuation):
        raise BridgeError("An explicit source normalization continuation is required")
    path = base / "incremental_manifest.json"
    if path.is_symlink() or not path.is_file():
        raise BridgeError("Continuation has no published parent incremental manifest")
    raw = path.read_bytes()
    if _sha(raw) != continuation.parent_increment_sha256:
        raise BridgeError("Continuation parent increment checksum changed")
    parent = json.loads(raw)
    previous = continuation.previous_batch
    if (parent.get("version_sha256") != _sha(_json_bytes({k: v for k, v in parent.items() if k != "version_sha256"}))
            or parent.get("status") != "bridge_verified"
            or parent.get("source") != batch.source or previous.source != batch.source
            or parent.get("generation") != previous.generation
            or parent.get("source_evidence") != previous.evidence
            or parent.get("policy") != json.loads(json.dumps(asdict(policy)))):
        raise BridgeError("Continuation parent source, generation, policy or evidence differs")
    prior, _, _ = _validated_batch(previous, policy)
    digest = _sha(prior.to_csv(index=False, float_format="%.17g").encode())
    if digest != parent.get("source_bars_sha256"):
        raise BridgeError("Continuation previous source bars differ from the published parent")
    # Include raw and adjusted prices, share volume, amount and trading status.
    # Tolerances are deliberately not used to authorize a historical revision.
    if list(prior.columns) != list(bars.columns):
        raise BridgeError("Continuation source schema changed")
    old = prior.set_index(["instrument", "date"])
    new = bars.set_index(["instrument", "date"])
    scales, counts = {}, {}
    for inst in sorted(bars.instrument.unique()):
        record = parent.get("securities", {}).get(inst)
        if record is None:
            continue  # First reconciliation for a newly audited security.
        if inst not in old.index.get_level_values("instrument"):
            raise BridgeError("Continuation lacks the previous source security: " + inst)
        left, right = old.loc[inst], new.loc[inst]
        common = left.index.intersection(right.index)
        if len(common) < policy.min_overlap:
            raise BridgeError("Continuation has fewer than required common source sessions: " + inst)
        if not left.loc[common].equals(right.loc[common]):
            raise BridgeError("Historical source revision requires separate verified evidence: " + inst)
        scale = record.get("normalization_scale")
        if isinstance(scale, bool) or not isinstance(scale, (int, float)) or not math.isfinite(scale) or scale <= 0:
            raise BridgeError("Continuation parent has an invalid normalization scale: " + inst)
        scales[inst] = float(scale)
        counts[inst] = len(common)
    return scales, {"kind": "verified_parent_source_normalization_continuation",
        "parent_increment_manifest": {"path": str(path.resolve()), "sha256": _sha(raw)},
        "previous_source": previous.source, "previous_generation": previous.generation,
        "previous_source_bars_sha256": digest, "current_generation": batch.generation,
        "unchanged_common_source_sessions": counts,
        "historical_source_revisions_accepted": False, "tolerances_relaxed": False,
        "scale_refitted": False}


def _audit(base_dir, batch, policy, continuation=None):
    base = Path(base_dir)
    old_calendar = _calendar(base)
    bars, calendar, sessions = _validated_batch(batch, policy)
    inherited, continuation_report = ({}, None) if continuation is None else _continuation_scales(
        base, batch, bars, policy, continuation)
    # No insertion/removal inside an existing calendar prefix is allowed. A
    # packaged future calendar may be retained, but never acts as a bar watermark.
    covered = [day for day in old_calendar if batch.requested_start <= day <= min(batch.requested_end, old_calendar[-1])]
    expected = [day for day in sessions if day <= old_calendar[-1]]
    if covered != expected:
        raise BridgeError("Source market calendar disagrees with the Qlib calendar overlap")
    if batch.requested_start > old_calendar[-1]:
        raise BridgeError("Source calendar must overlap the baseline; an unobserved interval cannot be guessed")
    new_calendar = old_calendar + [day for day in sessions if day > old_calendar[-1]]
    reports, converted = {}, {}
    for inst, group in bars.groupby("instrument", sort=True):
        old = read_qlib_security(base, inst, fields=policy.require_fields)
        inventory = _inventory(old)
        end = inventory["last_complete_finite"]
        if end is None:
            raise BridgeError(f"No complete baseline rows for {inst}")
        if old.loc[old.index > end].notna().any().any():
            raise BridgeError(f"Partial baseline tail needs reconciliation before append: {inst}")
        source = group.set_index("date")
        if end not in source.index:
            raise BridgeError(f"Source overlap does not include the baseline last finite session: {inst}")
        common = old.index.intersection(source.index)
        usable = old.loc[common].gt(0).all(axis=1) & old.loc[common].notna().all(axis=1) & ~source.loc[common, "suspended"]
        if (old.loc[common].gt(0).all(axis=1) & source.loc[common, "suspended"]).any():
            raise BridgeError(f"Source suspension contradicts finite baseline bars: {inst}")
        overlap = common[usable]
        if len(overlap) < policy.min_overlap:
            raise BridgeError(f"Fewer than {policy.min_overlap} valid overlap sessions: {inst}")
        unexplained = [day for day in common if day <= end and not usable.at[day] and not source.at[day, "suspended"]]
        if unexplained:
            raise BridgeError(f"Unexplained baseline gap/partial fields inside the overlap: {inst}")
        scale = inherited[inst] if inst in inherited else float(np.median(
            old.loc[overlap, "close"] / source.loc[overlap, "adjusted_close"]))
        if not math.isfinite(scale) or scale <= 0:
            raise BridgeError("Invalid cross-source normalization constant")
        qfactor = source.factor * scale
        output = pd.DataFrame(index=source.index)
        stats = {}
        for name in OHLC:
            output[name] = source[f"adjusted_{name}"] * scale
            stats[name] = _error_stats(old.loc[overlap, name], output.loc[overlap, name], policy.price_rtol, f"{inst}/{name}")
        output["factor"] = qfactor
        stats["factor"] = _error_stats(old.loc[overlap, "factor"], qfactor.loc[overlap], policy.factor_rtol, f"{inst}/factor")
        output["volume"] = source.raw_volume / (qfactor * policy.base_raw_volume_multiplier)
        stats["volume"] = _error_stats(old.loc[overlap, "volume"], output.loc[overlap, "volume"], policy.volume_rtol, f"{inst}/volume")
        if "vwap" in policy.require_fields:
            output["vwap"] = source.raw_amount / source.raw_volume * qfactor
            stats["vwap"] = _error_stats(old.loc[overlap, "vwap"], output.loc[overlap, "vwap"], policy.vwap_rtol, f"{inst}/vwap")
        output.loc[source.suspended, :] = np.nan
        append = output.loc[output.index > end]
        if not append.empty:
            needed = [day for day in new_calendar if end < day <= append.index[-1]]
            if append.index.tolist() != needed:
                raise BridgeError(f"Unobserved session gap between baseline and increment: {inst}")
        combined = old.reindex(new_calendar).copy()
        combined.loc[append.index, output.columns] = append
        inventory_after = _inventory(combined)
        old_fields = {path.name.removesuffix(".day.bin") for path in (base / "features" / inst.lower()).glob("*.day.bin")}
        reports[inst] = {"normalization_scale": scale, "overlap_valid_sessions": len(overlap),
            "overlap_start": overlap[0], "overlap_end": overlap[-1], "field_checks": stats,
            "before": inventory, "after": inventory_after, "source_observed_last_row": source.index[-1],
            "requested_end": batch.requested_end, "missing_tail_sessions": [day for day in sessions if day > source.index[-1]],
            "known_suspended_dates": source.index[source.suspended].tolist(), "new_rows": len(append),
            "new_finite_rows": int(append.notna().all(axis=1).sum()), "base_feature_sha256": old.attrs["checksums"],
            "nonupdated_extra_features": sorted(old_fields - set(policy.require_fields)),
            "unknown_base_gaps_are_not_market_holidays": True}
        if continuation is not None:
            reports[inst]["normalization_method"] = "fixed_verified_parent_scale" if inst in inherited else "initial_close_median"
        converted[inst] = combined
    result = {"schema_version": SCHEMA_VERSION, "status": "bridge_verified", "source": batch.source,
        "generation": batch.generation, "policy": asdict(policy), "source_evidence": batch.evidence,
        "source_bars_sha256": _sha(bars.to_csv(index=False, float_format="%.17g").encode()),
        "base_calendar_sha256": _sha((base / "calendars/day.txt").read_bytes()),
        "calendar_provenance": {"source": calendar.source, "source_url": calendar.source_url,
            "coverage_start": calendar.coverage_start, "coverage_end": calendar.coverage_end,
            "rows_sha256": _sha(_json_bytes([[day, flag] for day, flag in calendar.days]))},
        "calendar_tail_not_watermark": old_calendar[-1], "securities": reports,
        "audited_common_finite_end": min(item["after"]["last_complete_finite"] for item in reports.values()),
        "execution_ready": False, "references": REFERENCES,
        "limitations": ["Only declared securities and required fields passed this seam; other stocks/features are not advanced by association.",
                        "Unknown baseline gaps may be suspension or missing data; they remain explicit, never calendar holidays.",
                        "Passing a current overlap does not prove original historical source quality, total return or historical universe membership."]}
    if continuation_report is not None:
        result["normalization_continuation"] = continuation_report
    return result, converted, new_calendar


def audit_bridge(base_dir, batch, policy=None, *, continuation=None):
    """Read-only per-security seam audit; throws before publication on mismatch."""
    return _audit(base_dir, batch, policy or BridgePolicy(), continuation)[0]


def publish_increment(base_dir, batch, output_dir, policy=None, *, pilot_only=True, continuation=None):
    """Publish a new staged directory. No original base byte or index file changes.

    Pilot mode exports only audited securities with an explicitly fixed subset.
    Full-clone mode retains every other stock/index file exactly; it does not
    claim that their data or historical pool coverage advanced to the request end.
    """
    policy, base, target = policy or BridgePolicy(), Path(base_dir).resolve(), Path(output_dir).resolve()
    if target == base or base in target.parents or target in base.parents:
        raise BridgeError("New dataset must be outside and distinct from its immutable baseline")
    if target.exists():
        raise FileExistsError("Dataset version already exists")
    from .qlib_snapshot import guard_qlib_output_path
    guard_qlib_output_path(target)
    audit, converted, calendar = _audit(base, batch, policy, continuation)
    target.parent.mkdir(parents=True, exist_ok=True)
    import fcntl
    with (target.parent / ".qlib-increment-publish.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        if target.exists():
            raise FileExistsError("Dataset version already exists")
        with tempfile.TemporaryDirectory(prefix=".qlib-increment-stage-", dir=target.parent) as temporary:
            stage = Path(temporary) / "dataset"
            if pilot_only:
                (stage / "features").mkdir(parents=True)
                for inst in converted:
                    if any(path.is_symlink() for path in (base / "features" / inst.lower()).rglob("*")):
                        raise BridgeError("Baseline symlinks are not accepted during immutable cloning")
                    shutil.copytree(base / "features" / inst.lower(), stage / "features" / inst.lower())
                (stage / "instruments").mkdir()
            else:
                if any(path.is_symlink() for path in base.rglob("*")):
                    raise BridgeError("Baseline symlinks are not accepted during immutable cloning")
                shutil.copytree(base, stage)
            (stage / "calendars").mkdir(exist_ok=True)
            (stage / "calendars/day.txt").write_text("\n".join(calendar) + "\n")
            for inst, frame in converted.items():
                directory = stage / "features" / inst.lower()
                for name in policy.require_fields:
                    original, _ = _read_feature(base / "features" / inst.lower() / (name + ".day.bin"), _calendar(base))
                    finite = frame[name].dropna()
                    first = min(original.index[0], finite.index[0])
                    # Preserve original trailing NaNs; extend only for observed new
                    # rows, never to a future calendar or requested-end placeholder.
                    end = max(original.index[-1], audit["securities"][inst]["source_observed_last_row"])
                    values = frame.loc[first:end, name].to_numpy(dtype="<f4")
                    header = np.asarray([calendar.index(first)], dtype="<f4")
                    (directory / (name + ".day.bin")).write_bytes(header.tobytes() + values.tobytes())
                # Verify values at all pre-existing dates survived float32 output.
                reread = read_qlib_security(stage, inst, fields=policy.require_fields)
                for name in policy.require_fields:
                    before = read_qlib_security(base, inst, fields=(name,))[name]
                    before = before.loc[:audit["securities"][inst]["before"]["last_complete_finite"]]
                    check = reread.loc[before.index, name]
                    if not np.allclose(check, before, rtol=0, atol=0, equal_nan=True):
                        raise BridgeError("Staging changed a pre-existing baseline value")
            all_path = stage / "instruments/all.txt"
            existing = {} if pilot_only else {line.split("\t")[0].upper(): line for line in all_path.read_text().splitlines() if line.strip()}
            for inst, item in audit["securities"].items():
                existing[inst] = f"{inst}\t{item['after']['first_complete_finite']}\t{item['after']['last_complete_finite']}"
            all_path.write_text("\n".join(existing[key] for key in sorted(existing)) + "\n")
            # A parent's source declaration must not be inventoried and then
            # overwritten as though its old checksum described this new file.
            (stage / "incremental_manifest.json").unlink(missing_ok=True)
            manifest = {**audit, "created_at": datetime.now(timezone.utc).isoformat(), "base_dir": str(base),
                "scope": "fixed_retrospective_pilot_subset" if pilot_only else "base_clone_with_audited_security_updates",
                "not_a_full_market_update": True, "base_unchanged": True,
                "dataset_files": {str(path.relative_to(stage)): _sha(path.read_bytes()) for path in sorted(stage.rglob("*")) if path.is_file()}}
            manifest["version_sha256"] = _sha(_json_bytes(manifest))
            (stage / "incremental_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            # Recheck source inputs after copying, before final publication.
            if audit_bridge(base, batch, policy, continuation=continuation) != audit:
                raise BridgeError("Baseline/source changed while staging")
            os.rename(stage, target)
    return manifest
