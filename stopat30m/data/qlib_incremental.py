"""Audited cross-source Qlib seams and immutable staged incremental versions.

No downloads, source fallback, calendar guessing or in-place writes occur here.
The Qlib convention is qprice=raw_price*qfactor and qvolume*qfactor=raw_volume
(up to an explicitly declared original volume unit). A per-security constant
must reconcile all overlapping OHLC, factor, volume and required VWAP fields.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile

import numpy as np
import pandas as pd

from .historical_universe import normalize_member
from .research_calendar import Calendar, iso_day

OHLC = ("open", "high", "low", "close")
CORE_FIELDS = (*OHLC, "volume", "factor")
SCHEMA_VERSION = 1
REFERENCES = [
    "https://qlib.readthedocs.io/en/stable/component/data.html",
    "https://github.com/microsoft/qlib/blob/main/scripts/data_collector/yahoo/collector.py",
    "https://github.com/microsoft/qlib/blob/main/scripts/dump_bin.py",
]


class BridgeError(ValueError):
    pass


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class BridgePolicy:
    min_overlap: int = 20
    price_rtol: float = .0005
    factor_rtol: float = .0005
    volume_rtol: float = .001
    vwap_rtol: float = .001
    base_raw_volume_multiplier: float = 1.0
    require_fields: tuple[str, ...] = (*CORE_FIELDS, "vwap")

    def __post_init__(self):
        if isinstance(self.min_overlap, bool) or not isinstance(self.min_overlap, int) or self.min_overlap < 20:
            raise BridgeError("A seam requires at least 20 valid overlapping exchange sessions")
        for name in ("price_rtol", "factor_rtol", "volume_rtol", "vwap_rtol"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= .02:
                raise BridgeError("Bridge tolerances must be explicit finite values within (0,.02]")
        if isinstance(self.base_raw_volume_multiplier, bool) or self.base_raw_volume_multiplier not in (1.0, 100.0):
            raise BridgeError("Base volume units must be explicitly shares (1) or 100-share lots (100), never fitted")
        if not set(CORE_FIELDS).issubset(self.require_fields) or not set(self.require_fields).issubset({*CORE_FIELDS, "vwap"}):
            raise BridgeError("Required fields must contain OHLC/volume/factor and optionally VWAP")
        if len(set(self.require_fields)) != len(self.require_fields):
            raise BridgeError("Duplicate required feature fields")


@dataclass(frozen=True)
class CanonicalIncrement:
    bars: pd.DataFrame
    source: str
    generation: str
    calendar_rows: pd.DataFrame
    calendar_source: str
    calendar_source_url: str
    requested_start: str
    requested_end: str
    evidence: dict = field(default_factory=dict)
    volume_unit: str = "shares"

    @classmethod
    def from_frames(cls, raw, adjusted, *, source, generation, calendar_rows, calendar_source,
                    calendar_source_url, requested_start, requested_end, evidence):
        """Date+security alignment, never length-based pairing or factor=1 fallback."""
        frames = []
        for value, prefix in ((raw, "raw"), (adjusted, "adjusted")):
            current = value.copy()
            if "instrument" not in current and "code" in current:
                current = current.rename(columns={"code": "instrument"})
            if not {"date", "instrument", *OHLC}.issubset(current):
                raise BridgeError("Both source modes require explicit date/security and every OHLC field")
            current["date"] = current.date.map(iso_day)
            current["instrument"] = current.instrument.map(normalize_member)
            if current.duplicated(["date", "instrument"]).any():
                raise BridgeError("Duplicate source date/security rows")
            current = current.rename(columns={name: f"{prefix}_{name}" for name in OHLC})
            if prefix == "raw":
                current = current.rename(columns={"volume": "raw_volume", "amount": "raw_amount"})
                columns = ["date", "instrument", *[f"raw_{name}" for name in OHLC], "raw_volume"]
                columns += [name for name in ("raw_amount", "tradestatus") if name in current]
            else:
                columns = ["date", "instrument", *[f"adjusted_{name}" for name in OHLC]]
            if not set(columns).issubset(current):
                raise BridgeError("Raw share-volume is required; unknown units cannot be inferred")
            frames.append(current[columns])
        if set(map(tuple, frames[0][["date", "instrument"]].to_numpy())) != set(map(tuple, frames[1][["date", "instrument"]].to_numpy())):
            raise BridgeError("Raw/adjusted dates or instruments do not align")
        bars = frames[0].merge(frames[1], on=["date", "instrument"], validate="one_to_one")
        bars["factor"] = pd.to_numeric(bars.adjusted_close, errors="raise") / pd.to_numeric(bars.raw_close, errors="raise")
        return cls(bars.sort_values(["instrument", "date"]).reset_index(drop=True), source, generation,
                   calendar_rows.copy(), calendar_source, calendar_source_url,
                   requested_start, requested_end, evidence)


def from_baostock_probe(path, *, calendar_rows, calendar_source, calendar_source_url):
    """Consume a saved two-mode public probe with SHA receipts, without networking."""
    root = Path(path)
    receipt = json.loads((root / "receipt.json").read_text())
    if receipt.get("source") != "baostock" or receipt.get("status") != "complete":
        raise BridgeError("Expected a complete one-generation BaoStock probe")
    frames = {}
    for item in receipt["responses"]:
        name = item["file"]
        if Path(name).name != name:
            raise BridgeError("Invalid response receipt path")
        raw = (root / name).read_bytes()
        if _sha(raw) != item["sha256"]:
            raise BridgeError("Source response receipt checksum mismatch")
        payload = json.loads(raw)
        mode = item["mode"]
        if mode not in {"raw", "hfq"} or mode in frames:
            raise BridgeError("Probe must have exactly one raw and one adjusted response")
        expected = "3" if mode == "raw" else "1"
        if payload["request"].get("adjustflag") != expected:
            raise BridgeError("Probe adjustment mode does not match receipt")
        frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
        if not frame.adjustflag.astype(str).eq(expected).all():
            raise BridgeError("Source returned inconsistent per-row adjustment flags")
        if {normalize_member(value) for value in frame.code} != {receipt["instrument"]}:
            raise BridgeError("Source instrument differs from the probe receipt")
        frames[mode] = frame
    if set(frames) != {"raw", "hfq"}:
        raise BridgeError("Probe has incomplete raw/adjusted evidence")
    return CanonicalIncrement.from_frames(frames["raw"], frames["hfq"], source="baostock",
        generation=receipt["generation"], calendar_rows=calendar_rows, calendar_source=calendar_source,
        calendar_source_url=calendar_source_url, requested_start=receipt["request_start"],
        requested_end=receipt["request_end"], evidence={"source_url": "https://www.baostock.com/api/", "receipt": receipt})


def _calendar(base):
    path = Path(base) / "calendars/day.txt"
    days = [iso_day(line.strip()) for line in path.read_text().splitlines() if line.strip()]
    if not days or days != sorted(set(days)):
        raise BridgeError("Qlib calendar must be nonempty, sorted and unique")
    return days


def _read_feature(path, calendar):
    raw = path.read_bytes()
    if len(raw) < 8 or len(raw) % 4:
        raise BridgeError("Invalid Qlib float32 binary layout")
    values = np.frombuffer(raw, dtype="<f4")
    start = values[0]
    if not np.isfinite(start) or int(start) != start or start < 0 or int(start) + len(values) - 1 > len(calendar):
        raise BridgeError("Qlib feature header/range lies outside the declared calendar")
    if np.isinf(values[1:]).any():
        raise BridgeError("Qlib feature contains infinite values")
    return pd.Series(values[1:].astype(float), index=calendar[int(start):int(start) + len(values) - 1]), _sha(raw)


def read_qlib_security(base_dir, instrument, *, fields=(*CORE_FIELDS, "vwap")):
    symbol, base = normalize_member(instrument), Path(base_dir)
    calendar = _calendar(base)
    frame, checksums = pd.DataFrame(index=pd.Index(calendar, name="date")), {}
    for name in fields:
        if name not in {*CORE_FIELDS, "vwap", "change"}:
            raise BridgeError("Unknown audited Qlib field")
        path = base / "features" / symbol.lower() / (name + ".day.bin")
        if path.is_symlink() or not path.is_file():
            raise BridgeError(f"Missing required Qlib feature: {symbol}/{name}")
        frame[name], checksums[name] = _read_feature(path, calendar)
    frame.attrs["checksums"] = checksums
    return frame


def _inventory(frame):
    by_field = {}
    for name in frame:
        valid = frame[name].notna() & np.isfinite(frame[name]) & frame[name].gt(0)
        days = frame.index[valid].tolist()
        by_field[name] = {"first_finite": days[0] if days else None, "last_finite": days[-1] if days else None}
    valid = frame.notna().all(axis=1) & np.isfinite(frame).all(axis=1) & frame.gt(0).all(axis=1)
    days = frame.index[valid].tolist()
    gaps = frame.loc[days[0]:days[-1]].index[~valid.loc[days[0]:days[-1]]].tolist() if days else []
    partial = frame.notna().any(axis=1) & ~frame.notna().all(axis=1)
    return {"fields": by_field, "first_complete_finite": days[0] if days else None,
            "last_complete_finite": days[-1] if days else None, "internal_gap_dates": gaps,
            "gap_classification": "unknown_or_suspension_unless_source_status_verified",
            "partial_field_dates": frame.index[partial].tolist()}


def _validated_batch(batch, policy):
    if not batch.source or not batch.generation or not batch.evidence or batch.volume_unit != "shares":
        raise BridgeError("Single-source generation, share-volume units and provenance are required")
    calendar = Calendar.from_rows(batch.calendar_rows, batch.requested_start, batch.requested_end,
        source=batch.calendar_source, source_url=batch.calendar_source_url)
    sessions = [day for day, flag in calendar.days if flag]
    frame = batch.bars.copy()
    required = {"date", "instrument", "raw_volume", "factor", *[f"{prefix}_{name}" for prefix in ("raw", "adjusted") for name in OHLC]}
    if not required.issubset(frame) or frame.empty:
        raise BridgeError("Canonical increment lacks raw/adjusted OHLC, volume or factor")
    frame["date"] = frame.date.map(iso_day)
    frame["instrument"] = frame.instrument.map(normalize_member)
    if frame.duplicated(["date", "instrument"]).any() or not frame.date.isin(sessions).all():
        raise BridgeError("Increment has duplicate/off-calendar bars")
    for name, value in (("source", batch.source), ("generation", batch.generation)):
        if name in frame and not frame[name].eq(value).all():
            raise BridgeError("Mixed-source/generation bars cannot form one increment")
    numeric = list(required - {"date", "instrument"}) + (["raw_amount"] if "raw_amount" in frame else [])
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="raise").astype(float)
    status = frame.get("tradestatus", pd.Series("1", index=frame.index)).astype(str).str.lower()
    suspended_values = {"0", "0.0", "false", "suspended", "halted"}
    if not status.isin(suspended_values | {"1", "1.0", "true", "trading"}).all():
        raise BridgeError("Unknown trading status cannot be treated as a traded or suspended session")
    suspended = status.isin(suspended_values)
    trading = ~suspended
    if not np.isfinite(frame.loc[trading, numeric].to_numpy()).all() or not frame.loc[trading, numeric].gt(0).all().all():
        raise BridgeError("Missing/invalid traded-session numeric inputs")
    if not np.isclose(frame.loc[trading, "raw_volume"], np.rint(frame.loc[trading, "raw_volume"]), atol=1e-6, rtol=0).all():
        raise BridgeError("Raw share-volume must be integer-valued")
    for prefix in ("raw", "adjusted"):
        prices = frame.loc[trading, [f"{prefix}_{name}" for name in OHLC]]
        upper = prices[f"{prefix}_high"]
        lower = prices[f"{prefix}_low"]
        if (upper.lt(prices.max(axis=1) - 1e-6) | lower.gt(prices.min(axis=1) + 1e-6)).any():
            raise BridgeError("Source OHLC high/low do not bound the session prices")
    for name in OHLC:
        expected = frame.loc[trading, f"raw_{name}"] * frame.loc[trading, "factor"]
        if not np.isclose(frame.loc[trading, f"adjusted_{name}"], expected, rtol=policy.factor_rtol, atol=1e-6).all():
            raise BridgeError("Source adjustment is not one multiplicative OHLC factor; no affine-source fallback")
    if "vwap" in policy.require_fields and "raw_amount" not in frame:
        raise BridgeError("VWAP requires independently sourced raw CNY amount and share-volume")
    if "raw_amount" in frame:
        raw_vwap = frame.loc[trading, "raw_amount"] / frame.loc[trading, "raw_volume"]
        low, high = frame.loc[trading, "raw_low"], frame.loc[trading, "raw_high"]
        rounding = high.abs() * policy.price_rtol + 1e-6
        if (raw_vwap.lt(low - rounding) | raw_vwap.gt(high + rounding)).any():
            raise BridgeError("Source CNY amount/share-volume VWAP lies outside the raw OHLC range")
    frame["suspended"] = suspended
    for inst, group in frame.groupby("instrument"):
        expected = [day for day in sessions if group.date.min() <= day <= group.date.max()]
        if group.date.sort_values().tolist() != expected:
            raise BridgeError(f"Unexplained internal increment session gap: {inst}")
    return frame.sort_values(["instrument", "date"]), calendar, sessions


def _error_stats(observed, expected, tolerance, label):
    difference = np.abs(np.asarray(observed) - np.asarray(expected))
    relative = difference / np.maximum(np.abs(expected), np.finfo(float).tiny)
    passed = np.isclose(observed, expected, rtol=tolerance, atol=1e-7)
    if not np.all(passed):
        raise BridgeError(f"Cross-source seam mismatch in {label}: max relative error {float(np.max(relative)):.8g}")
    return {"max_relative_error": float(np.max(relative)), "rtol": tolerance, "points": int(len(relative))}


def _audit(base_dir, batch, policy):
    base = Path(base_dir)
    old_calendar = _calendar(base)
    bars, calendar, sessions = _validated_batch(batch, policy)
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
        scale = float(np.median(old.loc[overlap, "close"] / source.loc[overlap, "adjusted_close"]))
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
    return result, converted, new_calendar


def audit_bridge(base_dir, batch, policy=None):
    """Read-only per-security seam audit; throws before publication on mismatch."""
    return _audit(base_dir, batch, policy or BridgePolicy())[0]


def publish_increment(base_dir, batch, output_dir, policy=None, *, pilot_only=True):
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
    audit, converted, calendar = _audit(base, batch, policy)
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
            if audit_bridge(base, batch, policy) != audit:
                raise BridgeError("Baseline/source changed while staging")
            os.rename(stage, target)
    return manifest
