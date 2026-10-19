"""One fixed-parameter, forward-only refit on a verified unified snapshot.

This deliberately reuses already examined historical periods, including old
OOS dates. It cannot produce a new historical OOS claim. Only future paper
observations after publication can validate this candidate. Existing research
protocols, models and results are never edited.
"""
from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from stopat30m.backtest import model_comparison as ml
from stopat30m.backtest import walk_forward as wf
from stopat30m.data.historical_universe import HistoricalUniverse
from stopat30m.data.research_prices import load_research_prices

FORWARD_TRAINING_POLICY = "forward-only-mature-label-refit-v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
LIMITATIONS = [
    "This forward-only candidate deliberately fits already examined history, including previous validation/OOS periods; it has no new historical out-of-sample result.",
    "Validation/OOS date slots exist only for the legacy model loader: planned_future_not_observed. No future bars, labels, metrics or returns were invented.",
    "The frozen feature schema, model parameters and selection rule are reused without tuning or early stopping. Training-row counts are not independent observations.",
    "The current fixed ETF pool was selected retrospectively; expanding coverage does not establish historical point-in-time selection or remove survivorship bias.",
    "Adjusted fractional-exposure labels are not a broker execution model; verified-source checksums do not establish complete corporate actions or total-return reinvestment semantics.",
]


def _hash(value):
    return hashlib.sha256(ml._bytes(value)).hexdigest()


def _seal(value):
    return {**value, "protocol_sha256": _hash(value)}


def _instant(now):
    instant = now or datetime.now(timezone.utc)
    if not isinstance(instant, datetime) or instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("Forward training requires a timezone-aware execution time")
    return instant.astimezone(timezone.utc)


def _verified_snapshot(value, instant):
    path = getattr(value, "path", value)
    artifact = load_research_prices(path)
    if hasattr(value, "manifest_sha256") and (value.manifest_sha256 != artifact.manifest_sha256 or value.manifest != artifact.manifest):
        raise ValueError("Training source differs from its verified immutable snapshot")
    created = datetime.fromisoformat(artifact.manifest["created_at"])
    if created.tzinfo is None or created.utcoffset() is None or created > instant:
        raise ValueError("Training snapshot retrieval is naive or after the execution time")
    return artifact, created.astimezone(SHANGHAI)


def _common_cutoff(artifact, instruments, instant, fetched):
    calendar, frames, source = ml._inputs(artifact, instruments)
    current = instant.astimezone(SHANGHAI)
    # Today's bar qualifies only after the publication delay and a post-close
    # snapshot; pre-close acquisition must not make a partial daily bar mature.
    today_complete = current.time() >= time(15, 5) and fetched.date() == current.date() and fetched.time() >= time(15)
    last_allowed = current.date() if today_complete else current.date() - timedelta(days=1)
    candidates = calendar[calendar.date <= last_allowed]
    complete = pd.DataFrame(True, index=calendar, columns=instruments)
    for frame in frames.values():
        complete &= np.isfinite(frame) & frame.gt(0)
    if "tradestatus" in source:
        status = source.pivot(index="date", columns="instrument", values="tradestatus").reindex(index=calendar, columns=instruments)
        complete &= ~status.map(wf._suspended)
    common = candidates[complete.reindex(candidates).all(axis=1)]
    if common.empty:
        raise ValueError("No completed common exchange session for every frozen pool instrument")
    cutoff = common[-1].date().isoformat()
    last_by_instrument = {}
    for instrument in instruments:
        dates = candidates[complete.loc[candidates, instrument]]
        last_by_instrument[instrument] = dates[-1].date().isoformat() if len(dates) else None
    return cutoff, {"latest_calendar_date": calendar[-1].date().isoformat(),
        "last_complete_date_by_instrument": last_by_instrument,
        "excluded_calendar_sessions_after_cutoff": int((calendar > pd.Timestamp(cutoff)).sum()),
        "today_completed_bar_eligible": today_complete}


def _expanded_universe(universe, reference_pool, artifact, train_start, cutoff, future_end):
    original = wf._universe_payload(universe)
    if original["kind"] != "fixed":
        raise ValueError("Forward refitting currently requires the unchanged explicit fixed ETF pool")
    instruments = sorted({row["instrument"] for row in original["intervals"]})
    if instruments != sorted({row["instrument"] for row in reference_pool["intervals"]}):
        raise ValueError("Forward pool instruments differ from the frozen reference candidate")
    if train_start < original["coverage_start"]:
        raise ValueError("Do not invent fixed-pool coverage before its declared research start")
    payload = json.loads(ml._bytes(original))
    payload.pop("sha256", None)
    old_exclusive = (date.fromisoformat(original["coverage_end"]) + timedelta(days=1)).isoformat()
    new_end = max(original["coverage_end"], future_end)
    payload["coverage_end"] = new_end
    for interval in payload["intervals"]:
        if interval.get("effective_to") == old_exclusive:
            interval["effective_to"] = (date.fromisoformat(new_end) + timedelta(days=1)).isoformat()
    metadata = payload.setdefault("metadata", {})
    old_request = metadata.pop("research_data_request", None)
    metadata.update({"forward_training_policy": FORWARD_TRAINING_POLICY,
        "forward_training_origin_universe": original,
        "reference_data_request": old_request,
        "research_data_request": {"source": artifact.manifest["provider"]["name"],
            "calendar_source": artifact.manifest["calendar"]["source"],
            **{key: artifact.manifest["request"][key] for key in ("start_date", "end_date", "adjustment", "instruments")}}})
    expanded = HistoricalUniverse.from_dict(payload)
    audit = expanded.audit(train_start, cutoff, require_pit=False)
    return expanded, expanded.as_dict(), instruments, audit


def fit_forward_candidate(artifact_or_snapshot_path, universe, reference_model_protocol_path,
                          output_dir, *, train_start=None, now=None):
    """Publish a standard FrozenModelArtifact using only mature latest labels.

    ``universe`` may be the old fixed pool: its coverage is cloned/extended,
    retaining selection_date, existing membership starts and source evidence.
    The cutoff is calculated from verified common completed OHLCV, never chosen
    by historical return. The reference protocol supplies every model/feature,
    cost and selection parameter. Only the training date interval changes.

    The legacy loader requires disjoint train/validation/OOS ranges. The latter
    two are one-day *natural-date compatibility slots*, strictly after actual
    publication, labeled planned_future_not_observed, and never evaluated.
    """
    target = Path(output_dir)
    if target.exists():
        raise FileExistsError("Forward candidate output exists; never overwrite a frozen model")
    instant = _instant(now)
    reference, spec, old_cfg = ml._load_protocol(reference_model_protocol_path)
    artifact, fetched = _verified_snapshot(artifact_or_snapshot_path, instant)
    reference_pool = reference["base_protocol"]["universe"]
    instruments = sorted({row["instrument"] for row in reference_pool["intervals"]})
    declaration = reference_pool.get("metadata", {}).get("research_data_request", {})
    if declaration and (artifact.manifest["provider"]["name"] != declaration.get("source")
                        or artifact.manifest["request"]["adjustment"] != declaration.get("adjustment")):
        raise ValueError("Forward source/adjustment differs from the frozen reference candidate")
    if set(artifact.manifest["request"]["instruments"]) != set(instruments):
        raise ValueError("Unified training snapshot must contain exactly the frozen ETF pool")
    cutoff, coverage = _common_cutoff(artifact, instruments, instant, fetched)
    start = train_start or old_cfg.train_start
    if not isinstance(start, str) or date.fromisoformat(start).isoformat() != start or start > cutoff:
        raise ValueError("Require a valid training start no later than the common completed cutoff")
    if artifact.calendar[0] > pd.Timestamp(start):
        raise ValueError("Unified source begins after the declared training start")
    future = max(date.fromisoformat(cutoff), instant.astimezone(SHANGHAI).date()) + timedelta(days=1)
    validation, oos = future.isoformat(), (future + timedelta(days=1)).isoformat()
    expanded, pool, instruments, audit = _expanded_universe(universe, reference_pool, artifact, start, cutoff, oos)
    cfg = replace(old_cfg, train_start=start, train_end=cutoff, validation_start=validation,
                  validation_end=validation, oos_start=oos, oos_end=oos)
    declarations = {"training_role": "forward_only_no_historical_oos",
        "training_purpose": "forward_only_no_historical_oos",
        "planned_future_not_observed": True, "compatibility_slots_are_exchange_sessions": False,
        "historical_oos_is_retrospective": False,
        "reference_model_protocol_sha256": reference["protocol_sha256"],
        "reference_base_protocol_sha256": reference["base_protocol_sha256"],
        "training_input_snapshot_sha256": artifact.manifest["artifact_id"],
        "training_input_manifest_sha256": artifact.manifest_sha256,
        "training_implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    base = _seal({"schema_version": 1, "policy_version": wf.POLICY_VERSION,
        "config": asdict(cfg), "universe": pool, "universe_sha256": wf._digest(pool),
        "locked_at": instant.isoformat(), **declarations})
    protocol = {key: value for key, value in reference.items()
                if key not in {"protocol_sha256", "locked_at", "base_protocol", "base_protocol_sha256", "limitations", "model_selection"}}
    protocol.update({"base_protocol": base, "base_protocol_sha256": base["protocol_sha256"],
        "locked_at": instant.isoformat(), "model_selection": "one unchanged candidate refit for forward observations only; no validation, historical OOS evaluation or tuning",
        "limitations": LIMITATIONS, **declarations})
    protocol = _seal(protocol)
    ml._validate_protocol(protocol)

    features = ml.build_features(artifact, instruments, end_date=cutoff)
    labels = ml._labels(artifact, instruments, spec, cfg, end_date=cutoff)
    dataset = features.merge(labels, on=["signal_date", "instrument"], validate="one_to_one")
    in_period = dataset.signal_date.between(pd.Timestamp(start), pd.Timestamp(cutoff))
    mature = dataset.label_exit_date.le(pd.Timestamp(cutoff)) & dataset.label.notna()
    complete = np.isfinite(dataset.loc[:, ml.FEATURE_NAMES].to_numpy()).all(axis=1)
    eligible_cache = {}
    for signal, entry in dataset.loc[in_period & mature, ["signal_date", "label_entry_date"]].drop_duplicates().itertuples(index=False):
        eligible_cache[signal] = set(wf._members(expanded, signal, signal)) & set(wf._members(expanded, entry, signal))
    eligible = pd.Series([row.instrument in eligible_cache.get(row.signal_date, set()) for row in dataset.itertuples()], index=dataset.index)
    training = dataset.loc[in_period & mature & complete & eligible].copy().reset_index(drop=True)
    if len(training) < max(60, 2 * spec.min_data_in_leaf):
        raise ValueError("Insufficient mature, complete observations for the unchanged model parameters")
    lgb = ml._lgbm()
    train_set = lgb.Dataset(training.loc[:, ml.FEATURE_NAMES], label=training.label,
                            feature_name=list(ml.FEATURE_NAMES), free_raw_data=False)
    booster = lgb.train(spec.train_params(), train_set, num_boost_round=spec.num_boost_round)
    if booster.feature_name() != list(ml.FEATURE_NAMES):
        raise ValueError("Forward model feature order differs from the frozen schema")
    diagnostics = {"training_purpose": declarations["training_purpose"],
        "historical_periods_reused_for_fit": True, "validation": {"rows": 0, "status": "planned_future_not_observed", "used_for_fit": False},
        "final_oos": {"rows": 0, "status": "planned_future_not_observed", "used_for_fit": False},
        "historical_oos_metrics_available": False, "new_out_of_sample_performance_claimed": False}
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".forward-refit-", dir=target.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        booster.save_model(str(stage / "model.txt"))
        model_sha = hashlib.sha256((stage / "model.txt").read_bytes()).hexdigest()
        ml._csv(stage / "training.csv", training)
        ml._csv(stage / "features.csv", features)
        ml._csv(stage / "diagnostic_labels.csv", labels)
        ml._csv(stage / "predictions.csv", pd.DataFrame(columns=["signal_date", "instrument", "score", "status", "split", "training_period_in_sample", "model_sha256"]))
        ml._json_write(stage / "diagnostics.json", diagnostics)
        ml._json_write(stage / "protocol.json", protocol)
        files = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(stage.iterdir())}
        manifest = {"schema_version": 1, "policy_version": ml.MODEL_POLICY_VERSION,
            "created_at": instant.isoformat(), "trained_at": instant.isoformat(),
            "model_sha256": model_sha, "protocol_sha256": protocol["protocol_sha256"],
            "base_protocol_sha256": base["protocol_sha256"], "universe_sha256": base["universe_sha256"],
            "instruments": instruments, "feature_version": ml.FEATURE_VERSION, "feature_names": list(ml.FEATURE_NAMES),
            "feature_implementation_sha256": ml._code_hash(), "environment": ml._environment(lgb),
            "train_params": spec.train_params(), "model_spec": spec.as_dict(),
            "training": {"rows": len(training), "first_signal_date": str(training.signal_date.min().date()),
                "last_signal_date": str(training.signal_date.max().date()), "last_label_exit_date": str(training.label_exit_date.max().date()),
                "declared_cutoff": cutoff, "period_candidate_rows": int(in_period.sum()),
                "purged_or_missing_label_rows": int((in_period & ~mature).sum()),
                "excluded_missing_feature_rows": int((in_period & mature & ~complete).sum()),
                "excluded_ineligible_rows": int((in_period & mature & complete & ~eligible).sum()),
                "training_csv_sha256": files["training.csv"], "validation_used_for_fit": False,
                "final_oos_used_for_fit": False, "previous_historical_oos_reused_for_forward_fit": True,
                "early_stopping": False, "automatic_parameter_search": False},
            "source_snapshot": str(artifact.path), "source_manifest": artifact.manifest,
            "source_manifest_sha256": artifact.manifest_sha256, "universe_audit": audit,
            "common_completed_coverage": coverage, "files": files, "execution_ready": False,
            "limitations": LIMITATIONS, **declarations}
        manifest["manifest_sha256"] = _hash(manifest)
        ml._json_write(stage / "manifest.json", manifest)
        # Validate before reserving a new directory; publish the manifest last.
        ml.load_frozen_model(stage)
        target.mkdir(exist_ok=False)
        for path in sorted(stage.iterdir(), key=lambda path: path.name == "manifest.json"):
            path.replace(target / path.name)
    return ml.load_frozen_model(target)
