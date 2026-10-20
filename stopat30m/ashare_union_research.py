"""Fixed, annually refitted Alpha158 research over dated CSI300/CSI1000 members.

This is independent of frozen A3/A4 experiments. It neither chooses an execution
strategy nor changes an existing paper model. A declaration and verified market
audit are prerequisites for fitting, rather than conclusions inferred from PnL.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import gc
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import REQUIRED_FIELDS
from stopat30m.ashare_forward import LABEL, _processor_payload
from stopat30m.ashare_rolling_research import apply_frozen_processors, validate_training_rows
from stopat30m.ashare_forward import mature_signal_end
from stopat30m.data.ashare_scope import load_verified_historical_scope
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot, read_qlib_field
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm


POLICY = "a5-dated-csi300-csi1000-fixed-alpha158-annual-five-year-v1"
MARKET_CHECKS = ("scope_coverage", "scope_listing_validity", "raw_ohlc", "raw_preclose",
    "daily_trading_status", "historical_is_st", "ipo_identity", "calendar_coverage", "dividend_events",
    "ex_reference_explained", "source_vintage", "identity_events")


def _sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


def implementation_sha256():
    """Bind every project implementation used for fit, chronology or row policy."""
    root = Path(__file__).parent
    return {name: _sha(path) for name, path in {
        "union_training": Path(__file__), "rolling_helpers": root / "ashare_rolling_research.py",
        "forward_helpers": root / "ashare_forward.py", "native_trainer": root / "model/trainer.py",
        "scope_reader": root / "data/ashare_scope.py", "snapshot_reader": root / "data/qlib_snapshot.py",
        "official_scope_replay": root / "data/official_index_membership.py",
        "experiment_helpers": root / "ashare_experiment.py"}.items()}


def union_intervals(scope, start, end):
    """Merge overlapping index intervals without merging different identities."""
    first, last = pd.Timestamp(start), pd.Timestamp(end)
    if first > last:
        raise ValueError("Reversed union interval window")
    by_symbol = {}
    for pool in scope._pools.values():
        for symbol, intervals in pool.intervals.items():
            for left, right in intervals:
                if right >= first and left <= last:
                    by_symbol.setdefault(symbol, []).append((max(left, first), min(right, last)))
    result = {}
    for symbol, intervals in sorted(by_symbol.items()):
        merged = []
        for left, right in sorted(intervals):
            if merged and left <= merged[-1][1] + pd.Timedelta(days=1):
                merged[-1] = (merged[-1][0], max(right, merged[-1][1]))
            else:
                merged.append((left, right))
        result[symbol] = merged
    if not result:
        raise ValueError("No historical union members overlap this window")
    return result


def dated_membership_mask(index, intervals):
    """Filter by signal date only; future index survival never conditions a row."""
    if (not isinstance(index, pd.MultiIndex) or set(index.names) != {"datetime", "instrument"}
            or index.has_duplicates):
        raise ValueError("Dated membership requires unique datetime/instrument rows")
    days = pd.DatetimeIndex(index.get_level_values("datetime"))
    symbols = np.asarray(index.get_level_values("instrument"))
    keep = np.zeros(len(index), dtype=bool)
    for symbol in pd.unique(symbols):
        locations = np.flatnonzero(symbols == symbol)
        valid = np.zeros(len(locations), dtype=bool)
        for left, right in intervals.get(symbol, ()):
            valid |= (days[locations] >= left) & (days[locations] <= right)
        keep[locations] = valid
    return keep


class ObservedTrainingEligibility:
    """Actual source prices and legal code dates, with no constituent survival test."""

    def __init__(self, calendar, quote_complete, identities):
        self.calendar = pd.DatetimeIndex(pd.to_datetime(calendar))
        if self.calendar.empty or self.calendar.has_duplicates or not self.calendar.is_monotonic_increasing:
            raise ValueError("Eligibility requires a unique ordered actual-session calendar")
        self.quote_complete = quote_complete
        self.identities = identities
        for complete in quote_complete.values():
            if np.asarray(complete).shape != (len(self.calendar),):
                raise ValueError("Eligibility quote masks must align to the original calendar")

    def _identity_mask(self, symbol, days):
        identity = self.identities.get(symbol)
        if identity is None or identity.get("type") != "1" or not identity.get("ipoDate"):
            return np.zeros(len(days), dtype=bool)
        valid = days >= pd.Timestamp(identity["ipoDate"])
        if identity.get("outDate"):
            valid &= days < pd.Timestamp(identity["outDate"])
        return np.asarray(valid)

    def mask(self, index, intervals, *, cutoff=None, labels=None):
        """Training needs mature observed endpoints; inference uses signal data only.

        A stock leaving either index during its label remains a training sample
        when both future open endpoints exist. Future membership is not queried.
        """
        keep = dated_membership_mask(index, intervals)
        days = pd.DatetimeIndex(index.get_level_values("datetime"))
        symbols = np.asarray(index.get_level_values("instrument"))
        positions = self.calendar.get_indexer(days)
        failures = {"outside_signal_membership": int((~keep).sum())}
        missing_session = positions < 0
        failures["outside_actual_calendar"] = int((keep & missing_session).sum())
        keep &= ~missing_session
        identity_ok, quote_ok = np.zeros(len(index), bool), np.zeros(len(index), bool)
        endpoint_ok, mature = np.zeros(len(index), bool), np.ones(len(index), bool)
        training = cutoff is not None
        if training:
            if labels is None or len(labels) != len(index):
                raise ValueError("Training filtering requires aligned observed labels")
            cutoff_pos = self.calendar.get_indexer([pd.Timestamp(cutoff)])[0]
            if cutoff_pos < 6:
                raise ValueError("Training cutoff must be an observed mature source session")
            mature = (positions >= 0) & (positions + 6 <= cutoff_pos)
        for symbol in pd.unique(symbols):
            locations = np.flatnonzero((symbols == symbol) & (positions >= 0))
            identity_ok[locations] = self._identity_mask(symbol, days[locations])
            quotes = self.quote_complete.get(symbol)
            if quotes is None:
                continue
            quotes = np.asarray(quotes, dtype=bool)
            quote_ok[locations] = quotes[positions[locations]]
            if training:
                eligible = locations[mature[locations]]
                # Full endpoint bars are conservatively required; missing endpoint
                # opens cannot become a label by forward filling a suspended bar.
                left, right = positions[eligible] + 1, positions[eligible] + 6
                endpoint_ok[eligible] = (quotes[left] & quotes[right]
                    & self._identity_mask(symbol, self.calendar[left])
                    & self._identity_mask(symbol, self.calendar[right]))
        for name, valid in (("invalid_signal_identity", identity_ok), ("unobserved_signal_quote", quote_ok)):
            failures[name] = int((keep & ~valid).sum())
            keep &= valid
        if training:
            failures["immature_label"] = int((keep & ~mature).sum())
            keep &= mature
            failures["unobserved_or_illegal_label_endpoints"] = int((keep & ~endpoint_ok).sum())
            keep &= endpoint_ok
            label_values = np.asarray(labels, dtype=float)
            if label_values.shape != (len(index),):
                raise ValueError("Training labels must be a one-dimensional aligned series")
            label_valid = np.isfinite(label_values)
            failures["missing_label"] = int((keep & ~label_valid).sum())
            keep &= label_valid
        return keep, {"input_rows": len(index), "retained_rows": int(keep.sum()), "excluded_rows": int((~keep).sum()),
            "exclusions_in_application_order": failures, "future_membership_conditioned": False,
            "label_endpoint_checks": training}


def load_eligibility(snapshot, calendar, symbols, stock_basic_path):
    """Read all required Qlib fields and supplier identity rows without repairs."""
    path = Path(stock_basic_path)
    if path.suffix == ".csv":
        # This canonical CSV is derived by the audit, with all original registry
        # bytes retained separately in that audit's bound_inputs.
        registry = pd.read_csv(path, dtype=str, keep_default_na=False)
        fields, rows = list(registry.columns), registry.itertuples(index=False, name=None)
    else:
        payload = json.loads(path.read_text())
        if "fields" not in payload or "rows" not in payload:
            raise ValueError("Original stock-basic response schema is unavailable")
        fields, rows = payload["fields"], payload["rows"]
    if not {"code", "type", "ipoDate", "outDate"}.issubset(fields):
        raise ValueError("Stock-basic response lacks explicit identity dates/type")
    identities = {}
    for values in rows:
        if len(values) != len(fields):
            raise ValueError("Stock-basic row differs from source schema")
        row = dict(zip(fields, values))
        symbol = row["code"].replace(".", "").upper()
        if symbol in identities:
            raise ValueError("Duplicate source stock identities")
        identities[symbol] = row
    masks, source_files = {}, {}
    for symbol in sorted(symbols):
        complete = np.ones(len(calendar), dtype=bool)
        for field in REQUIRED_FIELDS:
            name = f"features/{symbol.lower()}/{field}.day.bin"
            path = snapshot.dataset_dir / name
            values = read_qlib_field(path, calendar)
            complete &= np.isfinite(values) & (values > 0)
            source_files[name] = snapshot.manifest["files"][name]["sha256"]
        masks[symbol] = complete
    return ObservedTrainingEligibility(calendar, masks, identities), source_files


def _raw_alpha158(snapshot, calendar, intervals, start, end):
    import qlib
    from qlib.contrib.data.handler import Alpha158
    from qlib.data.dataset.handler import DataHandlerLP
    qlib.init(provider_uri=str(snapshot.dataset_dir), region="cn", kernels=1,
              expression_cache=None, dataset_cache=None)
    location = int(calendar.searchsorted(pd.Timestamp(start)))
    if location < 80:
        raise ValueError("Insufficient actual Alpha158 warmup sessions")
    warmup = calendar[location - 80].date().isoformat()
    selected = {symbol: [(left, right) for left, right in spans
        if right >= pd.Timestamp(start) and left <= pd.Timestamp(end)] for symbol, spans in intervals.items()}
    selected = {symbol: spans for symbol, spans in selected.items() if spans}
    handler = Alpha158(instruments=selected, start_time=warmup, end_time=end,
        infer_processors=[], learn_processors=[], label=[LABEL])
    frame = handler.fetch(selector=slice(start, end), col_set=["feature", "label"], data_key=DataHandlerLP.DK_R)
    if (frame.empty or frame.index.has_duplicates or len(frame["feature"].columns) != 158
            or frame.index.get_level_values("datetime").min() < pd.Timestamp(start)
            or frame.index.get_level_values("datetime").max() > pd.Timestamp(end)):
        raise ValueError("Raw Alpha158 schema or date span differs")
    del handler
    gc.collect()
    return frame


def _train_dataset(frame, fold):
    from qlib.data.dataset import DatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    handler = DataHandlerLP(start_time=fold["train_start"], end_time=fold["train_signal_end"],
        data_loader=StaticDataLoader(frame),
        infer_processors=[{"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature",
            "fit_start_time": fold["train_start"], "fit_end_time": fold["train_signal_end"], "clip_outlier": True}},
            {"class": "Fillna", "kwargs": {"fields_group": "feature"}}],
        learn_processors=[{"class": "DropnaLabel"}, {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}}])
    return DatasetH(handler=handler, segments={"train": (fold["train_start"], fold["train_signal_end"])}), handler


def _load_protocol(path, source, scope_directory):
    protocol = json.loads(Path(path).read_text())
    training = protocol.get("training", {})
    if (protocol.get("kind") != "a5_union_fixed_research_protocol"
            or protocol.get("source_snapshot_manifest_sha256") != source.manifest_sha256
            or protocol.get("historical_scope_manifest_sha256") != _sha(Path(scope_directory) / "manifest.json")
            or training.get("execution_years") not in ([2023, 2024], [2023, 2024, 2025, 2026])
            or training.get("lookback_calendar_years") != 5 or training.get("purge_future_sessions") != 6
            or training.get("hyperparameter_search") is not False or training.get("early_stopping") is not False
            or training.get("label") != LABEL
            or training.get("fixed_lightgbm_spec") != asdict(FixedQlibLGBSpec())
            or training.get("membership_filter") != "signal_date_only_no_future_membership_survival_condition"):
        raise ValueError("A separately frozen fixed union research declaration is required")
    end = training.get("evaluation_end")
    if (not isinstance(end, str) or pd.Timestamp(end).date().isoformat() != end
            or pd.Timestamp(end).year != training["execution_years"][-1]):
        raise ValueError("Evaluation end must belong to the last declared execution year")
    return protocol, training


def union_fold_plan(calendar, years, *, evaluation_end):
    """Only a separately declared two-year or four-year, five-year rolling run."""
    days = pd.DatetimeIndex(pd.to_datetime(calendar))
    if (list(years) not in ([2023, 2024], [2023, 2024, 2025, 2026]) or days.empty
            or days.has_duplicates or not days.is_monotonic_increasing or pd.Timestamp(evaluation_end) not in days
            or pd.Timestamp(evaluation_end).year != years[-1]):
        raise ValueError("Union folds require declared years and a unique observed evaluation end")
    result = []
    for year in years:
        prior = days[days < pd.Timestamp(f"{year}-01-01")]
        current = days[(days >= pd.Timestamp(f"{year}-01-01"))
            & (days <= min(pd.Timestamp(f"{year}-12-31"), pd.Timestamp(evaluation_end)))]
        if prior.empty or current.empty:
            raise ValueError("Missing actual preceding/current source sessions")
        cutoff = prior[-1].date().isoformat()
        result.append({"execution_year": year, "train_start": f"{year - 5}-01-01", "data_cutoff": cutoff,
            "train_signal_end": mature_signal_end(days, cutoff), "signal_start": cutoff,
            "signal_end": current[-1].date().isoformat(), "first_execution_session": current[0].date().isoformat()})
    return result


def validate_market_audit(audit, *, source_sha, scope_sha, stock_basic_sha, coverage_start, coverage_end):
    """A prior CSI300 audit or an unbound 'passed' flag cannot authorize fitting."""
    if (audit.get("kind") != "a5_union_market_audit" or audit.get("status") != "passed"
            or audit.get("source_snapshot_manifest_sha256") != source_sha
            or audit.get("historical_scope_manifest_sha256") != scope_sha
            or audit.get("stock_basic_sha256") != stock_basic_sha
            or not isinstance(audit.get("start"), str) or not isinstance(audit.get("end"), str)
            or audit["start"] > coverage_start or audit["end"] < coverage_end
            or any(audit.get("checks", {}).get(check) is not True for check in MARKET_CHECKS)):
        raise ValueError("Complete verified, source-bound union market evidence is required before fitting")


def assert_market_originals_unchanged(audit, stock_basic_path):
    """Recheck original market bytes, rather than trusting a cached audit flag."""
    proofs = audit.get("bound_inputs")
    if not isinstance(proofs, dict) or not proofs or "stock_basic" not in proofs:
        raise ValueError("Market audit lacks original source bindings")
    basic = proofs["stock_basic"]
    if (Path(basic.get("path", "")).resolve() != Path(stock_basic_path).resolve()
            or basic.get("sha256") != audit.get("stock_basic_sha256")
            or audit.get("stock_basic_path") != str(Path(stock_basic_path).resolve())):
        raise ValueError("Training identity table differs from the audited original/derived source")
    for proof in proofs.values():
        path = Path(proof.get("path", ""))
        if not path.is_absolute() or not path.is_file() or _sha(path) != proof.get("sha256"):
            raise ValueError("Bound market source bytes changed since verification")


def build_union_predictions(snapshot_path, scope_directory, stock_basic_path, protocol_path, market_audit_path,
                            output_dir, *, progress=None):
    """Fit the declared two/four annual models after a source-bound market audit."""
    target = guard_qlib_output_path(output_dir)
    if target.exists():
        raise ValueError("Union training output exists; preserve original artifacts")
    source = load_qlib_snapshot(snapshot_path, verify=True)
    protocol, training = _load_protocol(protocol_path, source, scope_directory)
    calendar = pd.DatetimeIndex(pd.to_datetime((source.dataset_dir / "calendars/day.txt").read_text().splitlines()))
    folds = union_fold_plan(calendar, training["execution_years"], evaluation_end=training["evaluation_end"])
    audit = json.loads(Path(market_audit_path).read_text())
    validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=_sha(Path(scope_directory) / "manifest.json"), stock_basic_sha=_sha(stock_basic_path),
        coverage_start=folds[0]["first_execution_session"], coverage_end=training["evaluation_end"])
    assert_market_originals_unchanged(audit, stock_basic_path)
    inputs = {"declaration": {"path": str(Path(protocol_path).resolve()), "sha256": _sha(protocol_path)},
        "stock_basic": {"path": str(Path(stock_basic_path).resolve()), "sha256": _sha(stock_basic_path)},
        "market_audit": {"path": str(Path(market_audit_path).resolve()), "sha256": _sha(market_audit_path)}}
    implementation = implementation_sha256()
    if any(protocol.get("implementation_sha256", {}).get(name) != digest for name, digest in implementation.items()):
        raise ValueError("Union training implementation differs from its prior declaration")
    scope = load_verified_historical_scope(scope_directory, start=folds[0]["train_start"],
        end=training["evaluation_end"], require_official=True)
    sessions = [day.date().isoformat() for day in calendar if scope.start <= day.date().isoformat() <= scope.end]
    with scope.verified_batch():
        membership_audit = scope.audit_sessions(sessions)
        intervals = union_intervals(scope, scope.start, scope.end)
    if membership_audit["status"] != "passed":
        raise ValueError("Historical union membership audit must pass throughout training/inference")
    eligibility, source_files = load_eligibility(source, calendar, intervals, stock_basic_path)
    source_files["calendars/day.txt"] = source.manifest["files"]["calendars/day.txt"]["sha256"]
    target.mkdir(parents=True)
    shutil.copyfile(protocol_path, target / "declaration.json")
    shutil.copyfile(__file__, target / "union_training_implementation.py")
    _write(target / "membership_audit.json", membership_audit)
    all_scores, fold_records = [], []
    started = datetime.now(timezone.utc).isoformat()
    for fold in folds:
        year = fold["execution_year"]
        if progress:
            progress({"phase": "load_training_features", **fold})
        raw = _raw_alpha158(source, calendar, intervals, fold["train_start"], fold["data_cutoff"])
        keep, filtering = eligibility.mask(raw.index, intervals, cutoff=fold["data_cutoff"], labels=raw["label"].iloc[:, 0])
        filtered = raw.loc[keep].copy()
        del raw
        gc.collect()
        if filtered.empty:
            raise ValueError("No legal mature union training observations remain")
        dataset, handler = _train_dataset(filtered, fold)
        learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        maturity = validate_training_rows(learned, fold, calendar)
        if not dated_membership_mask(learned.index, intervals).all():
            raise ValueError("An out-of-signal-membership row reached model fitting")
        if progress:
            progress({"phase": "fit", "execution_year": year, **maturity, "filtering": filtering})
        model = fit_fixed_qlib_lightgbm(dataset, spec=FixedQlibLGBSpec())
        processors = _processor_payload(handler, model.feature_names)
        folder = target / f"LGB_{year}"
        folder.mkdir()
        fitted_at = datetime.now(timezone.utc).isoformat()
        model.save_native(folder / "model", metadata={"model": "LGB", "execution_year": year,
            "fitted_inference_processors": processors["processors"], "declaration_sha256": inputs["declaration"]["sha256"],
            "fit_role": "retrospective_reconstructed_history", "train_signal_end": fold["train_signal_end"]})
        _write(folder / "processors.json", processors)
        record = {**fold, **maturity, "training_filtering": filtering, "fitted_at": fitted_at,
            "model_sha256": _sha(folder / "model/model.txt"), "historical_model_freeze_claim": False}
        del filtered, learned, dataset, handler
        gc.collect()
        raw = _raw_alpha158(source, calendar, intervals, fold["signal_start"], fold["signal_end"])
        keep, inference_filter = eligibility.mask(raw.index, intervals)
        features = raw.loc[keep, "feature"].copy()
        del raw
        gc.collect()
        scores = apply_frozen_processors(features, model, processors["processors"]).rename("score").reset_index()
        scores["datetime"] = pd.to_datetime(scores["datetime"]).dt.strftime("%Y-%m-%d")
        scores["model"], scores["execution_year"] = "LGB", year
        scores = scores[["model", "execution_year", "datetime", "instrument", "score"]]
        scores.to_csv(folder / "predictions.csv", index=False, float_format="%.17g")
        all_scores.append(scores)
        record["inference_filtering"] = inference_filter
        _write(folder / "protocol.json", record)
        _write(folder / "manifest.json", {"schema_version": 1,
            "files": {str(file.relative_to(folder)): _sha(file) for file in sorted(folder.rglob("*")) if file.is_file()}})
        fold_records.append(record)
        if progress:
            progress({"phase": "fold_complete", "execution_year": year, "prediction_rows": len(scores), **maturity})
        del features, model, scores
        gc.collect()
    predictions = pd.concat(all_scores, ignore_index=True).sort_values(["model", "execution_year", "datetime", "instrument"])
    if predictions.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
        raise ValueError("Duplicate union scores within an execution-year fold")
    scope.assert_unchanged()
    if any(_sha(item["path"]) != item["sha256"] for item in inputs.values()):
        raise ValueError("Declared source inputs changed while training")
    if any(_sha(source.dataset_dir / name) != digest for name, digest in source_files.items()):
        raise ValueError("Model source feature bytes changed while training")
    if implementation_sha256() != implementation:
        raise ValueError("Declared implementation bytes changed while training")
    assert_market_originals_unchanged(audit, stock_basic_path)
    predictions.to_csv(target / "predictions.csv", index=False, float_format="%.17g")
    _write(target / "source_files.json", source_files)
    manifest = {"schema_version": 1, "kind": "a5_union_fixed_predictions", "policy": POLICY,
        "started_at": started, "completed_at": datetime.now(timezone.utc).isoformat(), "inputs": inputs,
        "source_snapshot": str(source.path), "source_manifest_sha256": source.manifest_sha256,
        "scope_sources": scope.source_receipts(), "folds": fold_records, "models": ["LGB"],
        "prediction_rows": len(predictions), "hyperparameter_search": False, "early_stopping": False,
        "historical_model_freeze_claim": False, "new_future_out_of_sample_claim": False,
        "active_paper_changed": False, "selection_performed": False,
        "implementation_sha256": implementation,
        "files": {str(file.relative_to(target)): _sha(file) for file in sorted(target.rglob("*")) if file.is_file()}}
    _write(target / "manifest.json", manifest)
    return {"manifest": manifest, "predictions": predictions, "path": target}
