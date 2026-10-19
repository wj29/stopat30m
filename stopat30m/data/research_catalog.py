"""Append-only ETF research datasets composed from verified public collections.

No network, price repair, model fitting, or supplier-vintage claims occur here.
The original history and HTTP responses stay immutable and independently usable.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .research_calendar import Calendar
from .research_http import _hash as response_hash
from .research_prices import (
    ResearchDataError, TencentResearchProvider, _calendar, _json_bytes,
    _manifest_identity, _pair, _price_quality, _sha, load_research_prices,
)

POOL = ("SH510300", "SH510500", "SZ159915")
_VALUES = [f"{mode}_{field}" for mode in ("raw", "adjusted") for field in ("open", "high", "low", "close")] + ["raw_volume", "raw_amount"]
_ENDPOINT = TencentResearchProvider.source_url


class CatalogError(ResearchDataError):
    pass


def _file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _read(path):
    path = Path(path)
    if path.is_symlink():
        raise CatalogError("Catalog inputs cannot be symlinks.")
    try:
        return json.loads(path.read_bytes())
    except (OSError, ValueError):
        raise CatalogError(f"Cannot validate research catalog JSON: {path.name}") from None


def _timestamp(value):
    try:
        stamp = datetime.fromisoformat(value)
        if stamp.tzinfo is None or stamp.timestamp() > datetime.now(timezone.utc).timestamp() + 1:
            raise ValueError
        return stamp
    except (ValueError, TypeError):
        raise CatalogError("Cached response retrieval timestamp is invalid.") from None


def _collection(cache_dir, generation, instruments):
    root = Path(cache_dir).absolute()
    if root.is_symlink():
        raise CatalogError("HTTP cache cannot be a symlink.")
    if not (root / "generations").is_dir() or (root / "generations").is_symlink():
        raise CatalogError("A real source generation directory is required.")
    candidates = sorted(p.name for p in (root / "generations").iterdir() if p.is_dir())
    if generation is None:
        if len(candidates) != 1:
            raise CatalogError("Specify one source generation; automatic cross-generation composition is forbidden.")
        generation = candidates[0]
    if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{32}", generation):
        raise CatalogError("Invalid HTTP source generation identifier.")
    folder = root / "generations" / generation
    if folder.is_symlink() or folder.parent.is_symlink():
        raise CatalogError("HTTP generation cannot escape through symlinks.")
    request_path = folder / "request.json"
    request = _read(request_path)
    scope = request.get("request", {})
    if request.get("generation") != generation or request.get("request_sha256") != response_hash(scope):
        raise CatalogError("Source generation request checksum mismatch.")
    provider, logical = scope.get("provider", {}), scope.get("request", {})
    if provider != {"name": "tencent", "version": TencentResearchProvider.version, "endpoint": _ENDPOINT}:
        raise CatalogError("Only explicitly identified Tencent source caches are supported.")
    if (logical.get("adjustment") != "hfq" or logical.get("frequency") != "1d"
            or not set(instruments).issubset(logical.get("instruments", []))):
        raise CatalogError("Recent source request does not match the frozen ETF HFQ pool.")
    active_path = root / "requests" / response_hash(scope) / "active.json"
    if not active_path.resolve().is_relative_to(root.resolve()) or (folder / "responses").is_symlink():
        raise CatalogError("Source cache paths cannot escape their declared root.")
    active = _read(active_path)
    if (active.get("generation") != generation or active.get("request") != scope
            or active.get("request_sha256") != response_hash(scope)
            or active.get("status") not in {"pending", "complete"}):
        raise CatalogError("Source active-generation identity mismatch.")
    created = _timestamp(active.get("created_at"))
    payloads, records = {}, []
    for path in sorted((folder / "responses").glob("*.json")):
        wrapper = _read(path)
        identity = wrapper.get("request", {})
        params = identity.get("params", {})
        if (set(identity) != {"endpoint", "params", "generation"} or identity.get("endpoint") != _ENDPOINT
                or identity.get("generation") != generation or path.stem != response_hash(identity)):
            raise CatalogError("Cached response request identity or filename mismatch.")
        if (wrapper.get("schema_version") != 1 or wrapper.get("payload_sha256") != response_hash(wrapper.get("payload"))
                or wrapper.get("record_sha256") != response_hash({key: wrapper.get(key) for key in ("request", "payload_sha256", "fetched_at")})):
            raise CatalogError("Cached response payload/metadata checksum mismatch.")
        fetched = _timestamp(wrapper.get("fetched_at"))
        if (fetched.timestamp() + 1 < created.timestamp()
                or fetched.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat() != active.get("as_of_date")):
            raise CatalogError("Cached response timestamp differs from its original collection window.")
        parts = str(params.get("param", "")).split(",")
        if (set(params) != {"_var", "param", "r"} or len(parts) != 6 or parts[1] != "day"
                or parts[4] != "640" or parts[5] not in {"", "hfq"}
                or parts[0].upper() not in instruments or parts[3] > logical["end_date"]):
            raise CatalogError("Cached Tencent window identity differs from the source request.")
        key = json.dumps(params, sort_keys=True)
        if key in payloads:
            raise CatalogError("Duplicate response request in the source collection.")
        payloads[key] = wrapper["payload"]
        records.append({"file": str(path), "file_sha256": _file_sha(path), "request": identity,
            "payload_sha256": wrapper["payload_sha256"], "record_sha256": wrapper["record_sha256"],
            "fetched_at": wrapper["fetched_at"]})
    if not records:
        raise CatalogError("No validated source responses are available.")
    return logical, payloads, {"generation": generation, "scope": scope,
        "request_file": str(request_path), "request_file_sha256": _file_sha(request_path),
        "active_file": str(active_path), "active_file_sha256": _file_sha(active_path),
        "first_response_at": min(r["fetched_at"] for r in records),
        "last_response_at": max(r["fetched_at"] for r in records), "responses": records}


class _Replay:
    def __init__(self, payloads):
        self.payloads = payloads
    def get(self, endpoint, **kwargs):
        key = json.dumps(kwargs["params"], sort_keys=True)
        if endpoint != _ENDPOINT or key not in self.payloads:
            raise CatalogError("A required source window is absent; no network fallback is permitted.")
        return SimpleNamespace(status_code=200, headers={}, text=json.dumps(self.payloads[key]))


class _Identities:
    source, source_url = "baostock", "https://www.baostock.com/"
    def __init__(self, values):
        self.values = values
    def instrument_info(self, instrument):
        info = copy.deepcopy(self.values[instrument])
        if info.get("asset_type") != "ETF":
            raise CatalogError("Incremental catalog requires parent-verified ETF identities.")
        return info


def _atomic_json(path, value):
    with tempfile.NamedTemporaryFile(prefix=".catalog-", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(_json_bytes(value))
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def catalog_entries(output_dir):
    """Read the small local version pointer and verify each immutable manifest."""
    root = Path(output_dir).absolute()
    index_path = root / "catalog.json"
    if not index_path.exists():
        return []
    index = _read(index_path)
    if index.get("schema_version") != 1 or index.get("sha256") != _sha(_json_bytes({k: v for k, v in index.items() if k != "sha256"})):
        raise CatalogError("Dataset catalog checksum mismatch.")
    versions = index.get("versions")
    if (not isinstance(versions, list) or not versions or not all(isinstance(entry, dict) for entry in versions)
            or index.get("current") != versions[-1].get("id")
            or len({entry.get("id") for entry in versions}) != len(versions)):
        raise CatalogError("Dataset catalog current pointer or version sequence is invalid.")
    for entry in versions:
        identifier = entry.get("id")
        if not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{64}", identifier):
            raise CatalogError("Invalid dataset catalog identifier.")
        artifact = load_research_prices(root / "snapshots" / identifier)
        if artifact.manifest_sha256 != entry["manifest_sha256"]:
            raise CatalogError("Dataset catalog manifest binding mismatch.")
    return copy.deepcopy(index["versions"])


def build_incremental_dataset(parent_snapshot, recent_cache_dir, calendar_path, output_dir, *,
                              instruments=POOL, generation=None, through=None):
    """Append a verified common prefix; refuse even one revised overlap value.

    ``calendar_path`` supplies genuine exchange sessions for the appended range.
    ``through=None`` advances only through the continuous all-security/raw/HFQ
    intersection. One-sided later bars remain in the original response cache.
    """
    parent = load_research_prices(parent_snapshot)
    symbols = tuple(sorted(set(instruments)))
    if not symbols or not set(symbols).issubset(parent.manifest["request"]["instruments"]):
        raise CatalogError("Selected ETF pool is absent from the historical parent.")
    if (parent.manifest["provider"]["name"] != "tencent" or parent.manifest["request"]["adjustment"] != "hfq"
            or parent.manifest["price_semantics"].get("adjustment_transform") != "affine_cash_accumulation"):
        raise CatalogError("Parent must be the verified Tencent cumulative-cash HFQ research dataset.")
    logical, payloads, collection = _collection(recent_cache_dir, generation, symbols)
    parent_end = parent.manifest["request"]["end_date"]
    if logical["start_date"] > parent_end or logical["end_date"] <= parent_end:
        raise CatalogError("Recent collection must overlap and extend the parent history.")
    calendar = Calendar.load(calendar_path)
    append_start = (pd.Timestamp(parent_end) + pd.Timedelta(days=1)).date().isoformat()
    sessions = calendar.sessions(append_start, logical["end_date"])
    if not sessions:
        raise CatalogError("No verified exchange sessions extend the historical parent.")
    replay = TencentResearchProvider(session=_Replay(payloads), calendar_provider=_Identities(parent.manifest["instruments"]),
        clock=lambda: 0, sleep=lambda delay: None, random=lambda: 0)
    raw, adjusted, security = {}, {}, {}
    for inst in symbols:
        raw[inst] = replay.fetch_raw(inst, logical["start_date"], logical["end_date"])
        adjusted[inst] = replay.fetch_adjusted(inst, logical["start_date"], logical["end_date"], "hfq")
        r, a = set(raw[inst].date), set(adjusted[inst].date)
        own = parent_end
        for day in sessions:
            if day not in r or day not in a:
                break
            own = day
        security[inst] = {"verified_through": own, "last_observed_date": max(r | a),
            "last_raw_date": max(r), "last_adjusted_date": max(a),
            "raw_only_dates": sorted(r - a), "adjusted_only_dates": sorted(a - r)}
    complete = parent_end
    for day in sessions:
        if any(day not in set(raw[inst].date) or day not in set(adjusted[inst].date) for inst in symbols):
            break
        complete = day
    if through is not None:
        through = pd.Timestamp(through).date().isoformat()
        if through > complete or through <= parent_end or not calendar.is_session(through):
            raise CatalogError("Requested watermark exceeds the verified all-security raw/HFQ session intersection.")
        complete = through
    if complete == parent_end:
        raise CatalogError("No complete incremental session can be published.")
    frames, audits, coverage = [], {}, {}
    for inst in symbols:
        r, a = raw[inst], adjusted[inst]
        r = r[r.date.le(complete)].copy(); a = a[a.date.le(complete)].copy()
        try:
            recent = _pair(r, a, inst, logical["start_date"], complete)
        except ResearchDataError as exc:
            raise CatalogError(f"Recent raw/HFQ source alignment failed for {inst}; rebuild that security explicitly: {exc}") from exc
        old = parent.prices[parent.prices.instrument.eq(inst)].copy()
        old["date"] = old.date.dt.strftime("%Y-%m-%d")
        overlap_old = old[old.date.ge(logical["start_date"])].set_index("date")
        overlap_new = recent[recent.date.le(parent_end)].set_index("date")
        if set(overlap_old.index) != set(overlap_new.index) or overlap_old.empty:
            raise CatalogError(f"Overlap date coverage changed for {inst}; rebuild that security's history explicitly.")
        overlap_new = overlap_new.reindex(overlap_old.index)
        mismatch = ~(overlap_old[_VALUES].to_numpy() == overlap_new[_VALUES].to_numpy())
        if mismatch.any():
            row, col = np.argwhere(mismatch)[0]
            raise CatalogError(f"Historical source revision for {inst} on {overlap_old.index[row]} field {_VALUES[col]}; rebuild that security explicitly.")
        audits[inst] = {"status": "exact_match", "first_date": min(overlap_old.index),
            "last_date": max(overlap_old.index), "rows": len(overlap_old), "columns": _VALUES,
            "compared_values": len(overlap_old) * len(_VALUES),
            "canonical_overlap_sha256": _sha(overlap_old[_VALUES].to_csv(float_format="%.17g").encode())}
        additions = recent[recent.date.gt(parent_end)]
        combined = pd.concat([old, additions], ignore_index=True).sort_values("date")
        quality = _price_quality(combined, inst, parent.manifest["instruments"][inst])
        coverage[inst] = {"row_count": len(combined), "first_date": combined.date.min(), "last_date": combined.date.max(),
            "appended_rows": len(additions), "raw_adjusted_aligned": True, "quality": quality,
            "historical_missing_sessions_preserved": parent.manifest["coverage"][inst].get("missing_sessions", [])}
        frames.append(combined)
    prices = pd.concat(frames, ignore_index=True).sort_values(["date", "instrument"]).reset_index(drop=True)
    old_calendar = parent.calendar_rows.copy()
    additions = pd.DataFrame([{"date": day, "is_trading_day": int(flag)} for day, flag in calendar.days if append_start <= day <= complete])
    all_calendar = _calendar(pd.concat([old_calendar, additions], ignore_index=True), parent.manifest["request"]["start_date"], complete)
    price_bytes = prices.to_csv(index=False, float_format="%.17g", lineterminator="\n", na_rep="").encode()
    calendar_bytes = all_calendar.to_csv(index=False, lineterminator="\n").encode()
    request = {**parent.manifest["request"], "instruments": list(symbols), "end_date": complete}
    lineage = {"atomic_supplier_vintage_verified": False,
        "parent": {"artifact_id": parent.manifest["artifact_id"], "manifest_sha256": parent.manifest_sha256,
            "path": str(parent.path), "files": copy.deepcopy(parent.manifest["files"])},
        "recent_collection": collection, "append_calendar": {"path": str(Path(calendar_path).absolute()),
            "file_sha256": _file_sha(calendar_path), **calendar.audit()},
        "overlap_audit": audits, "per_security": security,
        "verified_through": complete, "watermark_rule": "continuous_calendar_prefix_intersection_of_all_selected_raw_and_HFQ",
        "unpaired_later_bars_included": False, "only_new_dates_appended": True}
    manifest = {"schema_version": 1, "dataset_kind": "append_only_multi_collection", "request": request,
        "provider": copy.deepcopy(parent.manifest["provider"]),
        "created_at": datetime.now(timezone.utc).isoformat(), "execution_ready": False,
        "corporate_actions_complete": False, "point_in_time_adjustment_verified": False,
        "atomic_supplier_vintage_verified": False, "price_semantics": copy.deepcopy(parent.manifest["price_semantics"]),
        "calendar": {"source": "composed_verified_exchange_calendars", "source_url": "dataset_lineage",
            "natural_day_count": len(all_calendar), "session_count": int(all_calendar.is_trading_day.sum()),
            "parent": copy.deepcopy(parent.manifest["calendar"]), "appended_source": calendar.source},
        "files": {"prices.csv": {"rows": len(prices), "sha256": _sha(price_bytes)},
            "calendar.csv": {"rows": len(all_calendar), "sha256": _sha(calendar_bytes)}},
        "instruments": {inst: copy.deepcopy(parent.manifest["instruments"][inst]) for inst in symbols},
        "coverage": coverage, "dataset_lineage": lineage,
        "limitations": [*parent.manifest["limitations"],
            "This append-only dataset composes distinct supplier collections; exact observed overlap does not prove a single atomic supplier vintage.",
            "Historical missing/suspended bars remain missing; one-sided newer prices are excluded from the verified training watermark."]}
    identifier = _sha(_json_bytes(_manifest_identity(manifest)))
    manifest["artifact_id"] = identifier
    root = Path(output_dir).absolute()
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".catalog.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CatalogError("Research catalog is busy in another process.") from None
        versions = catalog_entries(root)
        snapshots = root / "snapshots"; snapshots.mkdir(exist_ok=True)
        target = snapshots / identifier
        if not target.exists():
            with tempfile.TemporaryDirectory(prefix=".dataset-", dir=snapshots) as temporary:
                staged = Path(temporary) / "snapshot"; staged.mkdir()
                (staged / "prices.csv").write_bytes(price_bytes)
                (staged / "calendar.csv").write_bytes(calendar_bytes)
                (staged / "manifest.json").write_bytes(_json_bytes(manifest))
                staged.rename(target)
        artifact = load_research_prices(target)
        if not any(entry["id"] == identifier for entry in versions):
            versions.append({"id": identifier, "manifest_sha256": artifact.manifest_sha256,
                "start_date": request["start_date"], "verified_through": complete,
                "instruments": list(symbols), "parent_id": parent.manifest["artifact_id"]})
        index = {"schema_version": 1, "current": versions[-1]["id"], "versions": versions}
        index["sha256"] = _sha(_json_bytes(index))
        _atomic_json(root / "catalog.json", index)
        return artifact


def publish_daily_update(parent_snapshot, recent_artifact, cache_dir, calendar_path, output_dir):
    """Publish a completed recent collection before forward settlement/signals.

    Prefer the last verified catalog version over the immutable seed. A repeated
    or older completed day must still match every available overlap value, then
    returns the current parent without moving its watermark or rewriting it.
    The exact original HTTP generation comes from the recent artifact manifest.
    """
    recent = load_research_prices(getattr(recent_artifact, "path", recent_artifact))
    if (recent.manifest["provider"].get("name") != "tencent"
            or set(recent.manifest["request"]["instruments"]) != set(POOL)
            or recent.manifest["request"].get("adjustment") != "hfq"):
        raise CatalogError("Daily publication requires a completed three-ETF Tencent HFQ artifact.")
    generation = recent.manifest.get("download_provenance", {}).get("generation")
    if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{32}", generation):
        raise CatalogError("Daily artifact lacks its exact original HTTP collection generation.")
    cache = Path(cache_dir).absolute()
    if not (cache / "generations").is_dir():
        cache = cache / "http" / "tencent"
    logical, _, collection = _collection(cache, generation, POOL)
    if logical != recent.manifest["request"]:
        raise CatalogError("Daily artifact request differs from its referenced source collection.")
    referenced = recent.manifest.get("download_provenance", {}).get("responses", [])
    expected = {_hash_record["request_sha256"]: _hash_record for _hash_record in referenced}
    actual = {response_hash(row["request"]): {"request_sha256": response_hash(row["request"]),
        "params": row["request"]["params"], "payload_sha256": row["payload_sha256"],
        "fetched_at": row["fetched_at"]} for row in collection["responses"]}
    if expected != actual:
        raise CatalogError("Daily artifact provenance differs from the original cached responses.")
    versions = catalog_entries(output_dir)
    parent_path = (Path(output_dir) / "snapshots" / versions[-1]["id"]) if versions else getattr(parent_snapshot, "path", parent_snapshot)
    parent = load_research_prices(parent_path)
    end = recent.manifest["request"]["end_date"]
    if end > parent.manifest["request"]["end_date"]:
        return build_incremental_dataset(parent.path, cache, calendar_path, output_dir,
            instruments=POOL, generation=generation, through=end)
    if parent.manifest["provider"] != recent.manifest["provider"] or parent.manifest["price_semantics"] != recent.manifest["price_semantics"]:
        raise CatalogError("Daily publication source/price semantics differ from the current master.")
    for inst in POOL:
        old = parent.prices[parent.prices.instrument.eq(inst)].set_index("date")
        update = recent.prices[recent.prices.instrument.eq(inst)].set_index("date")
        if update.empty or set(update.index) - set(old.index):
            raise CatalogError(f"Daily overlap date coverage differs for {inst}; explicit rebuild is required.")
        # Require the entire incoming interval to have exactly the same dates,
        # not merely the subset of bars a shortened provider happened to return.
        expected_dates = old.loc[recent.manifest["request"]["start_date"]:end].index
        if set(expected_dates) != set(update.index):
            raise CatalogError(f"Daily overlap is incomplete for {inst}; explicit rebuild is required.")
        if not np.array_equal(old.loc[update.index, _VALUES].to_numpy(), update[_VALUES].to_numpy()):
            raise CatalogError(f"Historical source revision for {inst}; rebuild that security explicitly.")
    return parent
