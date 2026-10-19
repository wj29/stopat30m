"""Independent, forward-only Alpha158 candidate and isolated A-share paper runs.

No real account, broker credential, ETF experiment or system job is touched.
Native models and JSON preprocessing state are frozen in a new directory.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, time, timezone
from decimal import Decimal, ROUND_FLOOR
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import PackageHistoricalPool, REQUIRED_FIELDS, _fitted_processors, _series
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm, load_fixed_qlib_model
from stopat30m.data.research_calendar import Calendar

SHANGHAI = ZoneInfo("Asia/Shanghai")
POLICY = "ashare-alpha158-forward-only-fixed-lightgbm-v1"
LABEL = "Ref($open,-6)/Ref($open,-1)-1"
LIMITATIONS = [
    "forward_only_no_historical_oos: this refit uses previously examined history and makes no new historical OOS claim.",
    "The official Alpha158 schema and fixed LightGBM parameters are reused without tuning, validation selection or early stopping.",
    "Package historical constituent intervals are not independently verified announcement-time PIT membership.",
    "Paper cash/share ledgers are isolated simulations, never actual broker orders or actual account balances.",
]


def _bytes(value):
    return json.dumps(_serial(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _serial(value):
    if isinstance(value, dict):
        return {str(key): _serial(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_serial(item) for item in value]
    if isinstance(value, (Decimal, Path)):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _digest(value):
    return hashlib.sha256(_bytes(value)).hexdigest()


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _instant(value=None):
    value = value or datetime.now(timezone.utc)
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A-share forward timestamps must include a timezone")
    return value.astimezone(timezone.utc)


def _day(value):
    if not isinstance(value, str) or pd.Timestamp(value).date().isoformat() != value:
        raise ValueError("A-share forward dates must be YYYY-MM-DD")
    return value


def _write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".ashare-", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write((json.dumps(_serial(value), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


@contextmanager
def _lock(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".ashare-forward.lock").open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another A-share forward operation is already running") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def mature_signal_end(calendar, cutoff):
    """Five open-to-open sessions need six future opens from signal close."""
    dates = pd.DatetimeIndex(pd.to_datetime(calendar))
    selected = pd.Timestamp(_day(cutoff))
    if dates.empty or dates.has_duplicates or not dates.is_monotonic_increasing or selected not in dates:
        raise ValueError("Cutoff must be an observed, unique source exchange session")
    location = dates.get_loc(selected)
    if location < 6:
        raise ValueError("Insufficient source sessions for mature five-session open labels")
    return dates[location - 6].date().isoformat()


def _snapshot_context(snapshot_path, start, cutoff):
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot
    snapshot = load_qlib_snapshot(snapshot_path, verify=True)
    dataset = snapshot.dataset_dir
    dates = pd.DatetimeIndex(pd.to_datetime((dataset / "calendars/day.txt").read_text().splitlines()))
    mature_end = mature_signal_end(dates, cutoff)
    pool = PackageHistoricalPool(dataset / "instruments/csi300.txt", start=start, end=cutoff)
    instruments = pool.qlib_intervals(start, cutoff)
    if not instruments or pd.Timestamp(start) < dates[0] or start > mature_end:
        raise ValueError("Training interval has no verified package historical members")
    files = {"calendars/day.txt", "instruments/csi300.txt"}
    for instrument in instruments:
        for field in REQUIRED_FIELDS:
            files.add(f"features/{instrument.lower()}/{field}.day.bin")
    hashes = {name: _sha(dataset / name) for name in sorted(files)}
    return {"snapshot": snapshot, "dataset": dataset, "calendar": dates, "pool": pool,
            "instruments": instruments, "mature_end": mature_end, "used_files": hashes}


def _training_dataset(context, start, cutoff):
    import qlib
    from qlib.contrib.data.handler import Alpha158
    from qlib.data.dataset import DatasetH
    qlib.init(provider_uri=str(context["dataset"]), region="cn", kernels=1,
              expression_cache=None, dataset_cache=None)
    calendar = context["calendar"]
    location = int(calendar.searchsorted(pd.Timestamp(start)))
    warmup = calendar[max(0, location - 80)].date().isoformat()
    end = context["mature_end"]
    handler = Alpha158(instruments=context["instruments"], start_time=warmup, end_time=cutoff,
        fit_start_time=start, fit_end_time=end, label=[LABEL],
        infer_processors=[{"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature",
            "fit_start_time": start, "fit_end_time": end, "clip_outlier": True}},
            {"class": "Fillna", "kwargs": {"fields_group": "feature"}}],
        learn_processors=[{"class": "DropnaLabel"}, {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}}])
    return DatasetH(handler=handler, segments={"train": (start, end)}), handler


def _processor_payload(handler, feature_names):
    states = _fitted_processors(handler)
    if [item["class"] for item in states] != ["RobustZScoreNorm", "Fillna"]:
        raise ValueError("Forward inference requires the frozen RobustZScoreNorm/Fillna schema")
    normalizer = states[0]
    mean = np.asarray(normalizer.get("mean_train"), dtype=float)
    std = np.asarray(normalizer.get("std_train"), dtype=float)
    if (mean.shape != (158,) or std.shape != (158,) or not np.isfinite(mean).all()
            or not np.isfinite(std).all() or (std <= 0).any() or not normalizer.get("clip_outlier")):
        raise ValueError("Fitted Alpha158 normalizer is incomplete or nonfinite")
    return {"schema_version": 1, "feature_names": list(feature_names), "processors": states,
            "fillna_value": 0.0, "clip_min": -3.0, "clip_max": 3.0}


def fit_ashare_forward_candidate(snapshot_path, output_dir, *, train_start="2012-01-01", cutoff, now=None):
    """Refit only mature labels, save a new native bundle, never historical OOS."""
    instant = _instant(now)
    start, cutoff = _day(train_start), _day(cutoff)
    local = instant.astimezone(SHANGHAI)
    if cutoff > local.date().isoformat() or (cutoff == local.date().isoformat() and local.time() < time(15, 5)):
        raise ValueError("Latest training cutoff must be a completed source session")
    target = Path(output_dir).resolve()
    if target.exists():
        raise FileExistsError("Forward candidate exists; never replace a frozen model")
    context = _snapshot_context(snapshot_path, start, cutoff)
    created = _instant(datetime.fromisoformat(context["snapshot"].manifest["created_at"]))
    if created > instant:
        raise ValueError("Training snapshot was published after the requested freeze time")
    dataset, handler = _training_dataset(context, start, cutoff)
    learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    days = pd.DatetimeIndex(learned.index.get_level_values("datetime"))
    if learned.empty or days.min() < pd.Timestamp(start) or days.max() > pd.Timestamp(context["mature_end"]):
        raise ValueError("Immature or out-of-range rows reached forward-only model fitting")
    model = fit_fixed_qlib_lightgbm(dataset, spec=FixedQlibLGBSpec())
    processor = _processor_payload(handler, model.feature_names)
    if any(_sha(context["dataset"] / name) != digest for name, digest in context["used_files"].items()):
        raise ValueError("Training source changed while fitting the candidate")
    freeze = _instant(now)
    import qlib
    protocol = {"policy": POLICY, "training_role": "forward_only_no_historical_oos", "label": LABEL,
        "holding_sessions": 5, "label_future_sessions": 6, "train_start": start,
        "data_cutoff": cutoff, "train_signal_end": context["mature_end"],
        "last_label_execution_end": cutoff, "purged_tail_sessions": 6,
        "model_spec": asdict(FixedQlibLGBSpec()), "parameters": FixedQlibLGBSpec().params(),
        "top_k": 20, "rebalance_sessions": 5, "momentum_lookback": 60,
        "feature_schema": "official_qlib_Alpha158_158", "feature_names": model.feature_names,
        "source_snapshot": str(context["snapshot"].path),
        "source_snapshot_manifest_sha256": context["snapshot"].manifest_sha256,
        "source_identity": context["snapshot"].manifest["source"],
        "price_semantics": context["snapshot"].manifest["price_semantics"],
        "used_files_sha256": context["used_files"], "qlib_version": qlib.__version__,
        "frozen_at": freeze.isoformat(), "historical_oos_claim": False,
        "membership_status": "package_declared_membership_not_full_PIT", "limitations": LIMITATIONS}
    protocol["protocol_sha256"] = _digest(protocol)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ashare-candidate-", dir=target.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        _write(stage / "protocol.json", protocol)
        _write(stage / "processors.json", processor)
        model.save_native(stage / "model", metadata={"protocol_sha256": protocol["protocol_sha256"],
            "training_role": protocol["training_role"], "fitted_inference_processors": processor["processors"]})
        manifest = {"schema_version": 1, "kind": "ashare_forward_candidate", "policy": POLICY,
            "training_role": protocol["training_role"], "frozen_at": freeze.isoformat(),
            "protocol_sha256": protocol["protocol_sha256"], "model_sha256": _sha(stage / "model/model.txt"),
            "training_rows": model.training_rows, "feature_names": model.feature_names,
            "data_cutoff": cutoff, "train_signal_end": context["mature_end"],
            "source_snapshot_manifest_sha256": context["snapshot"].manifest_sha256,
            "implementation_sha256": _sha(__file__), "execution_ready": False,
            "files": {str(path.relative_to(stage)): _sha(path) for path in sorted(stage.rglob("*")) if path.is_file()},
            "limitations": LIMITATIONS}
        manifest["manifest_sha256"] = _digest(manifest)
        _write(stage / "manifest.json", manifest)
        load_ashare_forward_candidate(stage)
        if target.exists():
            raise FileExistsError("Forward candidate already exists")
        stage.rename(target)
    return load_ashare_forward_candidate(target)


def _apply_fitted_feature_processors(features, processor_state):
    """Apply frozen Qlib statistics with its original dtype and operation order.

    JSON statistics have no numpy dtype. Alpha158 supplies a floating feature
    matrix, so restore those statistics to that matrix's dtype before each of
    Qlib's separate in-place subtraction and division operations. Promoting a
    float32 frame to float64 can change native LightGBM threshold decisions.
    This function never fits a processor or modifies the caller's frame.
    """
    states = processor_state["processors"]
    if ([state.get("class") for state in states] != ["RobustZScoreNorm", "Fillna"]
            or list(features.columns) != processor_state["feature_names"]
            or features.index.has_duplicates):
        raise ValueError("Inference Alpha158 feature order/index/processor schema differs from the frozen model")
    values = features.apply(pd.to_numeric, errors="raise").copy()
    dtype = values.to_numpy().dtype
    if not np.issubdtype(dtype, np.floating):
        raise ValueError("Inference Alpha158 features must retain their floating source dtype")
    if np.isinf(values.to_numpy()).any():
        raise ValueError("Infinite inference features are invalid source observations")
    normalizer = states[0]
    mean = np.asarray(normalizer["mean_train"], dtype=dtype)
    std = np.asarray(normalizer["std_train"], dtype=dtype)
    if (mean.shape != (len(features.columns),) or std.shape != mean.shape
            or not np.isfinite(mean).all() or not np.isfinite(std).all() or (std <= 0).any()
            or not normalizer.get("clip_outlier") or processor_state.get("fillna_value") != 0
            or processor_state.get("clip_min") != -3 or processor_state.get("clip_max") != 3):
        raise ValueError("Frozen inference normalizer is incomplete or differs from the native schema")
    values -= mean
    values /= std
    return values.clip(-3, 3).fillna(0)


class FrozenAshareCandidate:
    def __init__(self, path, manifest, protocol, processor, model):
        self.path, self.manifest, self.protocol = Path(path), manifest, protocol
        self.processor, self.model = processor, model
        self.model_sha256 = manifest["model_sha256"]

    def predict_features(self, features):
        """Apply saved statistics only. Future frames never refit preprocessing."""
        load_ashare_forward_candidate(self.path)
        if list(features.columns) != self.manifest["feature_names"] or features.index.has_duplicates:
            raise ValueError("Inference Alpha158 feature order/index differs from the frozen model")
        values = _apply_fitted_feature_processors(features, self.processor)
        scores = self.model.booster.predict(values, num_threads=1)
        if not np.isfinite(scores).all():
            raise ValueError("Forward model returned nonfinite predictions")
        return pd.Series(scores, index=features.index, name="score")

    def predict(self, snapshot_path, signal_date, instruments):
        from stopat30m.data.qlib_snapshot import load_qlib_snapshot
        snapshot = load_qlib_snapshot(snapshot_path, verify=True)
        if (snapshot.manifest["source"] != self.protocol["source_identity"]
                or snapshot.manifest["price_semantics"] != self.protocol["price_semantics"]):
            raise ValueError("Forward inference source/basis differs from the frozen candidate")
        features = _prediction_features(snapshot, _day(signal_date), instruments)
        return _series(self.predict_features(features))


def _prediction_features(snapshot, signal_date, instruments):
    import qlib
    from qlib.contrib.data.handler import Alpha158
    from qlib.data.dataset import DatasetH
    dates = pd.DatetimeIndex(pd.to_datetime((snapshot.dataset_dir / "calendars/day.txt").read_text().splitlines()))
    day = pd.Timestamp(signal_date)
    if day not in dates or not instruments or any(not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", inst) for inst in instruments):
        raise ValueError("Prediction needs explicit valid source-session instruments")
    location = dates.get_loc(day)
    if location < 80:
        raise ValueError("Insufficient official Alpha158 warmup sessions")
    qlib.init(provider_uri=str(snapshot.dataset_dir), region="cn", kernels=1, expression_cache=None, dataset_cache=None)
    handler = Alpha158(instruments=sorted(set(instruments)), start_time=dates[location - 80], end_time=day,
                       infer_processors=[], learn_processors=[], label=[LABEL])
    dataset = DatasetH(handler=handler, segments={"predict": (day, day)})
    frame = dataset.prepare("predict", col_set="feature", data_key="infer")
    if isinstance(frame.columns, pd.MultiIndex):
        frame = frame["feature"]
    if set(pd.DatetimeIndex(frame.index.get_level_values("datetime"))) != {day}:
        raise ValueError("Prediction returned dates outside the frozen close signal")
    return frame


def load_ashare_forward_candidate(path):
    root = Path(path).resolve()
    payload = json.loads((root / "manifest.json").read_text())
    if (payload.get("schema_version") != 1 or payload.get("kind") != "ashare_forward_candidate"
            or payload.get("policy") != POLICY or payload.get("training_role") != "forward_only_no_historical_oos"
            or payload.get("manifest_sha256") != _digest({k: v for k, v in payload.items() if k != "manifest_sha256"})):
        raise ValueError("Frozen A-share candidate manifest is invalid")
    for name, digest in payload["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or _sha(root / relative) != digest:
            raise ValueError("Frozen A-share model/preprocessor checksum mismatch")
    protocol = json.loads((root / "protocol.json").read_text())
    if protocol.get("protocol_sha256") != payload["protocol_sha256"] or protocol["protocol_sha256"] != _digest({k: v for k, v in protocol.items() if k != "protocol_sha256"}):
        raise ValueError("Frozen A-share protocol checksum mismatch")
    import qlib
    if protocol["qlib_version"] != qlib.__version__:
        raise ValueError("Official Alpha158 runtime differs from the frozen candidate")
    processor = json.loads((root / "processors.json").read_text())
    native_manifest = json.loads((root / "model/manifest.json").read_text())
    if native_manifest["numpy_version"] != np.__version__ or native_manifest["pandas_version"] != pd.__version__:
        raise ValueError("Frozen Alpha158 numerical runtime differs from the candidate")
    model = load_fixed_qlib_model(root / "model")
    if (len(model.feature_names) != 158 or model.feature_names != payload["feature_names"]
            or processor["feature_names"] != model.feature_names or payload["model_sha256"] != _sha(root / "model/model.txt")
            or asdict(model.spec) != asdict(FixedQlibLGBSpec())):
        raise ValueError("Frozen A-share Alpha158/model parameter schema differs")
    return FrozenAshareCandidate(root, payload, protocol, processor, model)


def _evidence(items):
    """Hash actual local receipts, never a source URL presented as saved bytes."""
    if not isinstance(items, list) or not items:
        raise ValueError("Actual local market/universe evidence files and checksums are required")
    verified = []
    for item in items:
        original = Path(item["path"])
        path = original.resolve()
        if original.is_symlink() or not path.is_file() or _sha(path) != item.get("sha256"):
            raise ValueError("Market/universe evidence checksum mismatch")
        verified.append({**item, "path": str(path)})
    return verified


def _save_state(root, state):
    unsigned = {key: value for key, value in state.items() if key != "state_sha256"}
    state["state_sha256"] = _digest(unsigned)
    _write(Path(root) / "state.json", state)


def _read_paper(root):
    from stopat30m.backtest import ashare_execution as execution
    root = Path(root).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    if (manifest.get("schema_version") != 1 or manifest.get("kind") != "isolated_ashare_integer_forward_paper" or manifest.get("policy") != POLICY
            or manifest.get("manifest_sha256") != _digest({key: value for key, value in manifest.items() if key != "manifest_sha256"})):
        raise ValueError("A-share paper manifest checksum/schema mismatch")
    if _sha(root / "calendar.json") != manifest["calendar_sha256"]:
        raise ValueError("Frozen source exchange calendar changed")
    if _sha(execution.__file__) != manifest["execution_implementation_sha256"]:
        raise ValueError("Frozen integer execution implementation changed; explicit migration is required")
    candidate = load_ashare_forward_candidate(root / "candidate")
    if candidate.manifest["manifest_sha256"] != manifest["candidate_manifest_sha256"]:
        raise ValueError("Paper candidate differs from its frozen manifest")
    state = json.loads((root / "state.json").read_text())
    if (state.get("state_sha256") != _digest({key: value for key, value in state.items() if key != "state_sha256"})
            or state.get("manifest_sha256") != manifest["manifest_sha256"]):
        raise ValueError("A-share paper state checksum mismatch")
    for item in state["receipts"]:
        if _sha(item["path"]) != item["sha256"]:
            raise ValueError("Committed A-share paper receipt changed")
    for account in state["accounts"].values():
        execution.PortfolioState.from_dict(account)
    return root, manifest, state, candidate, Calendar.load(root / "calendar.json")


def initialize_ashare_forward(candidate_dir, paper_dir, calendar_path, *, instruments,
        universe_evidence, initial_capital=100000, now=None, execution_policy=None,
        execution_source="tencent.public_level1_observation", min_eligible=20):
    """Create two independent 100k paper ledgers, never real account rows/jobs."""
    from stopat30m.backtest import ashare_execution as execution
    candidate = load_ashare_forward_candidate(candidate_dir)
    instant = _instant(now)
    if _instant(datetime.fromisoformat(candidate.manifest["frozen_at"])) > instant:
        raise ValueError("Candidate was frozen after the paper initialization clock")
    calendar = Calendar.load(calendar_path)
    local_day = instant.astimezone(SHANGHAI).date().isoformat()
    first = calendar.next_session(local_day)
    pool = sorted(set(instruments))
    if (not pool or len(pool) != len(instruments)
            or any(not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", instrument) for instrument in pool)
            or isinstance(min_eligible, bool) or not isinstance(min_eligible, int) or min_eligible < 20):
        raise ValueError("Require an explicit unique stock universe and at least 20 eligible candidates")
    evidence = _evidence(universe_evidence)
    policy = execution_policy or execution.ExecutionPolicy(initial_cash=execution.decimal(initial_capital))
    if policy.initial_cash != execution.decimal(initial_capital):
        raise ValueError("Initial capital differs from the frozen execution policy")
    target = Path(paper_dir).resolve()
    if target.exists():
        raise FileExistsError("Paper directory exists; never reset balances or initialization")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ashare-paper-", dir=target.parent) as temporary:
        stage = Path(temporary) / "paper"
        stage.mkdir()
        shutil.copytree(candidate.path, stage / "candidate")
        shutil.copyfile(calendar_path, stage / "calendar.json")
        manifest = {"schema_version": 1, "kind": "isolated_ashare_integer_forward_paper", "policy": POLICY,
            "created_at": instant.isoformat(), "first_possible_execution": first,
            "initial_universe": pool, "universe_evidence": evidence,
            "universe_scope": "current_source_snapshot_for_prospective_signals_only_not_backdated_training_membership",
            "min_eligible": min_eligible, "initial_capital_per_strategy": str(policy.initial_cash),
            "strategies": ["model", "momentum"], "execution_policy": policy.to_dict(),
            "execution_implementation_sha256": _sha(execution.__file__),
            "execution_source": execution_source, "open_window": ["09:30", "09:45"],
            "max_quote_age_seconds": min(60, policy.max_live_age_seconds), "daily_attempt_limit": 3,
            "calendar_sha256": _sha(stage / "calendar.json"),
            "candidate_manifest_sha256": candidate.manifest["manifest_sha256"],
            "model_sha256": candidate.model_sha256, "initial_live_order_count": 0,
            "execution_ready": False, "limitations": [*LIMITATIONS,
                "Execution uses the fresh observed price at the paper job time, never a retrospectively filled opening price.",
                "Missing/stale new-buy candidates are excluded before signal freeze with reasons. Later missing execution quotes never replace the selected names.",
                "Missing held closes produce partial/stale NAV and block that account's next orders until a complete close is available."]}
        manifest["manifest_sha256"] = _digest(manifest)
        _write(stage / "manifest.json", manifest)
        state = {"schema_version": 1, "manifest_sha256": manifest["manifest_sha256"],
            "accounts": {name: execution.PortfolioState.initial(policy).to_dict() for name in manifest["strategies"]},
            "signals": {}, "completed": {}, "attempts": {}, "receipts": [], "valuations": [],
            "current_universe": pool, "corporate_actions": {},
            "observation_gaps": {},
            "unresolved_actions": {name: {} for name in manifest["strategies"]},
            "last_success": None, "last_failure": None}
        _save_state(stage, state)
        _read_paper(stage)
        if target.exists():
            raise FileExistsError("Paper directory already exists")
        stage.rename(target)
    return ashare_forward_status(target)


def _batch(batch, phase, session, clock, manifest):
    if (not isinstance(batch, dict) or batch.get("phase") != phase or batch.get("session") != session
            or not batch.get("source") or not isinstance(batch.get("quotes"), dict)
            or not isinstance(batch.get("metadata"), dict)):
        raise ValueError("Market batch phase/session/provenance schema differs")
    fetched = _instant(datetime.fromisoformat(batch["fetched_at"]))
    if fetched > clock or fetched.astimezone(SHANGHAI).date().isoformat() != session:
        raise ValueError("Market batch retrieval is future-dated or from another session")
    if phase == "open" and (batch["source"] != manifest["execution_source"]
            or (clock - fetched).total_seconds() > manifest["max_quote_age_seconds"]):
        raise ValueError("Live batch is stale or from another execution source")
    if phase == "close" and fetched.astimezone(SHANGHAI).time() < time(15, 5):
        raise ValueError("Close batch was not collected after session completion")
    _evidence(batch.get("evidence"))
    return batch


def _status(value):
    return {"1": "trading", "0": "suspended"}.get(str(value), str(value))


def _close_price(quote, session, clock):
    if not isinstance(quote, dict) or _status(quote.get("tradestatus")) != "trading":
        return None, "source_suspended_or_unknown"
    try:
        if quote.get("session", quote.get("date")) != session or not quote.get("source"):
            return None, "wrong_session_or_no_source"
        fetched = _instant(datetime.fromisoformat(quote["fetched_at"]))
        if fetched > clock or fetched.astimezone(SHANGHAI).date().isoformat() != session or fetched.astimezone(SHANGHAI).time() < time(15, 5):
            return None, "not_a_postclose_retrieval"
        asof = quote.get("as_of", quote.get("asof"))
        if asof is not None:
            observed = _instant(datetime.fromisoformat(asof))
            local = observed.astimezone(SHANGHAI)
            if observed > clock or local.date().isoformat() != session or local.time() < time(15):
                return None, "stale_or_intraday_source_time"
        elif quote.get("bar_complete") is not True:
            return None, "unknown_source_time_or_daily_completion"
        from stopat30m.backtest.ashare_execution import decimal
        value = decimal(quote.get("close", quote.get("latest", quote.get("execution_price"))))
        factor = decimal(quote.get("factor"))
        reference = decimal(quote.get("preclose"))
        if value <= 0 or factor <= 0 or reference <= 0:
            return None, "nonpositive_raw_close"
        return value, None
    except (ValueError, KeyError, TypeError):
        return None, "invalid_raw_close_or_timestamp"


def _security_metadata(rows, session, clock):
    from stopat30m.backtest.ashare_execution import SecurityInfo, BOARDS
    from dataclasses import fields
    allowed = {field.name for field in fields(SecurityInfo)}
    valid, rejected = {}, {}
    for instrument, row in rows.items():
        try:
            info = SecurityInfo(**{key: value for key, value in row.items() if key in allowed})
            if (info.instrument != instrument or info.status_session != session or not info.source
                    or not isinstance(info.is_st, bool) or info.board not in BOARDS
                    or _day(info.listing_date) > session or info.delisting_period is not False
                    or _instant(datetime.fromisoformat(info.known_at)) > clock):
                raise ValueError("Unknown/current-session security metadata is required")
            valid[instrument] = info
        except (ValueError, TypeError, KeyError):
            rejected[instrument] = "missing_or_unknown_current_security_metadata"
    return valid, rejected


def _candidate_targets(candidate, snapshot_path, signal_date, eligible):
    scores = candidate.predict(snapshot_path, signal_date, eligible).xs(pd.Timestamp(signal_date), level="datetime")
    if set(scores.index) != set(eligible) or not np.isfinite(scores).all():
        raise ValueError("Frozen model lacks finite scores for the eligible signal-day universe")
    model_order = sorted(scores.items(), key=lambda pair: (-float(pair[1]), pair[0]))[:20]
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot
    from qlib.data import D
    snapshot = load_qlib_snapshot(snapshot_path)
    calendar = pd.DatetimeIndex(pd.to_datetime((snapshot.dataset_dir / "calendars/day.txt").read_text().splitlines()))
    location = calendar.get_loc(pd.Timestamp(signal_date))
    if location < 60:
        raise ValueError("Momentum baseline needs 60 preceding source sessions")
    close = D.features(instruments=eligible, fields=["$close"], start_time=calendar[location - 60], end_time=signal_date)
    table = close.reset_index().pivot(index="datetime", columns="instrument", values="$close").reindex(calendar[location - 60:location + 1])
    momentum, omitted = {}, {}
    for instrument in eligible:
        window = table[instrument]
        if window.isna().any() or not np.isfinite(window).all() or not window.gt(0).all():
            omitted[instrument] = "incomplete_60_session_momentum_history"
        else:
            momentum[instrument] = float(window.iloc[-1] / window.iloc[0] - 1)
    momentum_order = sorted(((key, value) for key, value in momentum.items() if value > 0), key=lambda pair: (-pair[1], pair[0]))[:20]
    selected = {"model": model_order, "momentum": momentum_order}
    return {name: _equal_weights([key for key, _ in rows]) for name, rows in selected.items()}, {
        "model_scores": {key: float(value) for key, value in scores.items()}, "momentum_scores": momentum,
        "momentum_exclusions": omitted}


def _equal_weights(instruments):
    if not instruments:
        return {}
    weight = (Decimal(1) / len(instruments)).quantize(Decimal("0.000000000000000001"), rounding=ROUND_FLOOR)
    return {key: str(weight) for key in instruments}


def _signal_bar_eligibility(candidate, batch, instruments, session):
    """A fresh raw quote alone cannot make an old/missing model bar current."""
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot
    from stopat30m.data.qlib_incremental import read_qlib_security
    if not batch.get("snapshot_path"):
        return [], {key: "no_verified_current_model_snapshot" for key in instruments}, None
    snapshot = load_qlib_snapshot(batch["snapshot_path"], verify=True)
    if _instant(datetime.fromisoformat(snapshot.manifest["created_at"])) > _instant(datetime.fromisoformat(batch["fetched_at"])):
        raise ValueError("Signal snapshot was published after the recorded input batch")
    if (snapshot.manifest["source"] != candidate.protocol["source_identity"]
            or snapshot.manifest["price_semantics"] != candidate.protocol["price_semantics"]):
        raise ValueError("Signal snapshot source/basis differs from the frozen model")
    valid, rejected = [], {}
    for instrument in instruments:
        try:
            frame = read_qlib_security(snapshot.dataset_dir, instrument)
            row = frame.loc[session]
            quote = batch["quotes"][instrument]
            raw_close = float(quote.get("close", quote.get("latest", quote.get("execution_price"))))
            factor = float(quote["factor"])
            if (not np.isfinite(row.to_numpy(dtype=float)).all() or not row.gt(0).all()
                    or not np.isclose(row["close"] / row["factor"], raw_close, rtol=.0005, atol=1e-6)
                    or not np.isclose(row["factor"], factor, rtol=.0005, atol=1e-8)):
                raise ValueError("Current source/model bar mismatch")
            valid.append(instrument)
        except (ValueError, KeyError, OSError):
            rejected[instrument] = "missing_or_mismatched_current_qlib_bar"
    return valid, rejected, snapshot.manifest_sha256


def _account_instruments(account):
    """Retain quotes/evidence for rights after the original shares are sold."""
    return (set(account["positions"])
        | {row["instrument"] for row in account.get("claims", {}).values()}
        | {row["instrument"] for row in account.get("entitlements", {}).values() if row.get("status") == "pending"})


def _action_book(state, batch, clock):
    """Freeze economic terms and the first actual source response for replay."""
    from stopat30m.backtest.ashare_execution import decimal
    rows = batch.get("corporate_actions", [])
    if not isinstance(rows, list):
        raise ValueError("Corporate actions must be an explicit event list")
    evidence = _evidence(batch["evidence"])
    hashes = {item["sha256"] for item in evidence}
    book = state.setdefault("corporate_actions", {})
    for row in rows:
        event = {key: row.get(key) for key in ("event_id", "instrument", "record_date", "ex_date",
            "pay_date", "listing_date", "known_at", "source_response_sha256")}
        if (not isinstance(event["event_id"], str) or not event["event_id"]
                or not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", str(event["instrument"]))
                or event["source_response_sha256"] not in hashes
                or _instant(datetime.fromisoformat(event["known_at"])) > clock):
            raise ValueError("Corporate action identity, known time or actual source evidence is invalid")
        for field in ("record_date", "ex_date"):
            _day(event[field])
        for field in ("pay_date", "listing_date"):
            if event[field] is not None:
                _day(event[field])
        cash, bonus = decimal(row.get("cash_per_share")), decimal(row.get("bonus_ratio"))
        stock_bonus, reserve_bonus = None, None
        if bonus:
            stock_bonus = decimal(row.get("stock_dividend_ratio"))
            reserve_bonus = decimal(row.get("capital_reserve_ratio"))
            if stock_bonus < 0 or reserve_bonus < 0 or stock_bonus + reserve_bonus != bonus:
                raise ValueError("Bonus shares require evidenced stock-dividend versus capital-reserve classification")
        tax_rate = row.get("cash_dividend_tax_rate")
        tax_evidence = row.get("tax_evidence_sha256")
        if tax_rate is not None:
            tax_rate = decimal(tax_rate)
            if not 0 <= tax_rate <= 1 or tax_evidence not in hashes:
                raise ValueError("Cash cannot cover evidenced dividend tax, or explicit tax evidence is missing")
        elif tax_evidence is not None:
            raise ValueError("Explicit dividend tax evidence needs its effective tax rate")
        if stock_bonus and (not cash or tax_rate is None):
            raise ValueError("Taxable stock dividend needs evidenced nominal-value tax and sufficient cash cover")
        reference_cash = row.get("reference_cash_per_share")
        reference_bonus = row.get("reference_bonus_ratio")
        reference_evidence = row.get("reference_evidence_sha256")
        if reference_cash is not None or reference_bonus is not None:
            reference_cash = decimal(reference_cash) if reference_cash is not None else None
            reference_bonus = decimal(reference_bonus) if reference_bonus is not None else None
            if (reference_evidence not in hashes or reference_cash is not None and reference_cash < 0
                    or reference_bonus is not None and reference_bonus < 0):
                raise ValueError("Exchange reference overrides require nonnegative terms and actual source evidence")
        elif reference_evidence is not None:
            raise ValueError("Exchange reference evidence needs its explicit adjustment terms")
        if (cash < 0 or bonus < 0 or not (cash or bonus)
                or event["record_date"] >= event["ex_date"]
                or (cash and (event["pay_date"] is None or event["pay_date"] < event["ex_date"]))
                or (bonus and (event["listing_date"] is None or event["listing_date"] < event["ex_date"]))):
            raise ValueError("Corporate action economic terms or dates are invalid")
        event.update(cash_per_share=str(cash), bonus_ratio=str(bonus),
            stock_dividend_ratio=str(stock_bonus) if stock_bonus is not None else "0",
            capital_reserve_ratio=str(reserve_bonus) if reserve_bonus is not None else "0",
            cash_dividend_tax_rate=str(tax_rate) if tax_rate is not None else None,
            tax_evidence_sha256=tax_evidence,
            reference_cash_per_share=str(reference_cash) if reference_cash is not None else None,
            reference_bonus_ratio=str(reference_bonus) if reference_bonus is not None else None,
            reference_evidence_sha256=reference_evidence)
        previous = book.get(event["event_id"])
        if previous:
            # A daily response may have new bytes/generation, but may not revise
            # already frozen terms or change the evidence bound to an entitlement.
            if any(previous.get(key) != value for key, value in event.items() if key != "source_response_sha256"):
                raise ValueError("Corporate action id has conflicting frozen economic terms")
        else:
            event["evidence"] = [item for item in evidence if item["sha256"] in
                {event["source_response_sha256"], tax_evidence, reference_evidence}]
            book[event["event_id"]] = event
    for event in book.values():
        _evidence(event["evidence"])
    return sorted(book.values(), key=lambda row: (row["ex_date"], row["event_id"]))


def _apply_actions(account, events, *, manifest, session, calendar, clock):
    """Recognize ex-date rights before orders; settle only evidenced due dates."""
    from stopat30m.backtest import ashare_execution as execution
    records = []
    for event in events:
        instrument, event_id = event["instrument"], event["event_id"]
        if event["ex_date"] > session:
            continue
        if event["record_date"] < _instant(datetime.fromisoformat(manifest["created_at"])).astimezone(SHANGHAI).date().isoformat():
            records.append({"event_id": event_id, "status": "no_preinitialization_entitlement"})
            continue  # This isolated account started in cash, without prior shares.
        relevant = (instrument in account.positions
            or any(row.get(instrument, 0) for row in account.holdings_history.values())
            or any(row["instrument"] == instrument for row in account.claims.values())
            or any(row["instrument"] == instrument for row in account.entitlements.values()))
        if not relevant:
            continue
        recorded = account.holdings_history.get(event["record_date"])
        existing_right = any(identity in account.entitlements for identity in (event_id + ":cash", event_id + ":bonus"))
        prior_claim = any(row["instrument"] == instrument and row["created_date"] <= event["record_date"] for row in account.claims.values())
        if recorded is not None and recorded.get(instrument, 0) == 0 and not existing_right and not prior_claim:
            records.append({"event_id": event_id, "status": "no_record_date_ownership"})
            continue
        common = {"instrument": instrument, "record_date": event["record_date"], "session": session,
            "known_at": event["known_at"],
            "evidence_sha256": event.get("tax_evidence_sha256") or event["source_response_sha256"],
            "calendar": calendar, "now": clock}
        for kind, amount, due, register, settle in (
            ("cash", event["cash_per_share"], event["pay_date"], execution.register_dividend_entitlement, execution.apply_cash_dividend),
            ("bonus", event["bonus_ratio"], event["listing_date"], execution.register_bonus_entitlement, execution.apply_bonus_shares)):
            if execution.decimal(amount) == 0:
                continue
            identity = event_id + ":" + kind
            terms = ({"pay_date": due, "cash_per_share": amount,
                "tax_rate": event.get("cash_dividend_tax_rate") or manifest["execution_policy"]["dividend_tax_rate"]} if kind == "cash"
                else {"listing_date": due, "ratio": amount})
            registered = account.entitlements.get(identity)
            if not registered:
                if session != event["ex_date"]:
                    raise ValueError("Missed evidenced ex-date entitlement; refuse retrospective corporate-action posting")
                action = register(account, event_id=identity, ex_date=event["ex_date"], **common, **terms)
                account = action.state
                records.append({"event_id": event_id, "operation": "register_" + kind,
                    "replayed": action.replayed, "record": action.record})
            if due < session and account.entitlements[identity]["status"] == "pending":
                raise ValueError("Missed evidenced settlement date; refuse retrospective corporate-action posting")
            if due == session and account.entitlements[identity]["status"] == "pending":
                action = settle(account, event_id=identity, **common, **terms)
                account = action.state
                records.append({"event_id": event_id, "operation": "settle_" + kind,
                    "replayed": action.replayed, "record": action.record})
    return account, records


def _verified_action_reference(account, instrument, session, reference, events):
    from stopat30m.backtest.ashare_execution import decimal
    rows = [row for row in account.entitlements.values()
        if row["instrument"] == instrument and row.get("ex_date") == session]
    bases = {row["pre_ex_carried_mark"] for row in rows if row.get("pre_ex_carried_mark") is not None}
    if len(bases) != 1 or reference is None:
        return False
    matching = [row for row in events if row["instrument"] == instrument and row["ex_date"] == session]
    cash = sum((decimal(row.get("reference_cash_per_share") if row.get("reference_cash_per_share") is not None
        else row["cash_per_share"]) for row in matching), Decimal(0))
    bonus = sum((decimal(row.get("reference_bonus_ratio") if row.get("reference_bonus_ratio") is not None
        else row["bonus_ratio"]) for row in matching), Decimal(0))
    expected = (decimal(next(iter(bases))) - cash) / (1 + bonus)
    return abs(expected - decimal(reference)) < Decimal("0.01")


def _record_observation_gaps(state, manifest, calendar, session, clock):
    """Missing whole close observations are permanent audit gaps, never halts."""
    initialization = _instant(datetime.fromisoformat(manifest["created_at"])).astimezone(SHANGHAI).date().isoformat()
    observed = {row["session"] for row in state["valuations"]}
    gaps = state.setdefault("observation_gaps", {})
    changed = False
    for expected in calendar.sessions(calendar.coverage_start, calendar.coverage_end):
        if initialization <= expected < session and expected not in observed and expected not in gaps:
            gaps[expected] = {"session": expected, "detected_at": clock.isoformat(),
                "reason": "missing_whole_session_close_observation",
                "policy": "all_new_orders_frozen_until_explicit_audit_or_new_paper_period_no_backfill"}
            changed = True
    return changed


def _close_operation(root, manifest, state, candidate, calendar, batch, clock):
    from stopat30m.backtest import ashare_execution as execution
    session = batch["session"]
    universe = batch.get("instruments", state["current_universe"])
    if (not isinstance(universe, list) or len(universe) != len(set(universe))
            or any(not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", key) for key in universe)):
        raise ValueError("Close batch needs an explicit current source universe")
    if sorted(universe) != state["current_universe"] and not batch.get("universe_evidence"):
        raise ValueError("A changed current source universe requires its own evidence")
    current_evidence = _evidence(batch.get("universe_evidence", manifest["universe_evidence"]))
    info, exclusions = _security_metadata(batch["metadata"], session, clock)
    prices, eligible = {}, []
    needed = set(universe) | set().union(*(_account_instruments(account) for account in state["accounts"].values()))
    for instrument in sorted(needed):
        price, reason = _close_price(batch["quotes"].get(instrument), session, clock)
        prices[instrument] = price
        if instrument in universe:
            if reason:
                exclusions[instrument] = reason
            elif instrument not in info:
                exclusions.setdefault(instrument, "missing_security_metadata")
            else:
                eligible.append(instrument)
    eligible, model_exclusions, snapshot_sha256 = _signal_bar_eligibility(candidate, batch, eligible, session)
    exclusions.update(model_exclusions)
    sessions = calendar.sessions(calendar.coverage_start, calendar.coverage_end)
    next_session = calendar.next_session(session)
    location = sessions.index(next_session) - sessions.index(manifest["first_possible_execution"])
    rebalance = location >= 0 and location % candidate.protocol["rebalance_sessions"] == 0
    reasons, valuations, allowed = [], {}, {}
    policy = execution.ExecutionPolicy(**manifest["execution_policy"])
    events = _action_book(state, batch, clock)
    action_records = {}
    for name in manifest["strategies"]:
        account = execution.PortfolioState.from_dict(state["accounts"][name])
        account, action_records[name] = _apply_actions(account, events, manifest=manifest,
            session=session, calendar=sessions, clock=clock)
        account_prices = dict(prices)
        unresolved = state["unresolved_actions"][name]
        previous = calendar.previous_session(session)
        for instrument, position in account.positions.items():
            quote = batch["quotes"].get(instrument, {})
            reference = quote.get("preclose")
            reference_verified = _verified_action_reference(account, instrument, session, reference, events)
            if reference_verified:
                unresolved.pop(instrument, None)
            if (position.last_mark_date == previous and position.last_mark is not None and reference is not None
                    and not reference_verified
                    and abs(execution.decimal(reference) - position.last_mark) >= Decimal("0.01")):
                unresolved.setdefault(instrument, {"detected_session": session,
                    "previous_raw_close": str(position.last_mark), "source_preclose": str(reference),
                    "reason": "verified_cash_or_share_entitlement_required"})
            if instrument in unresolved:
                account_prices[instrument] = None
                reasons.append(f"{name}:unresolved_corporate_action:{instrument}")
        suspended = [key for key in account.positions if _status(batch["quotes"].get(key, {}).get("tradestatus")) == "suspended"
            and key in info and info[key].status_session == session]
        result = execution.mark_to_close(account, session=session, prices=account_prices, calendar=sessions,
                                        policy=policy, verified_suspended=suspended)
        state["accounts"][name] = result.state.to_dict()
        allowed[name] = (result.valuation_complete and len(eligible) >= manifest["min_eligible"]
                         and not state.get("observation_gaps"))
        valuations[name] = _serial({key: value for key, value in vars(result).items() if key != "state"})
        if not result.valuation_complete:
            reasons.append(f"{name}:partial_held_valuation_freezes_orders")
        if len(eligible) < manifest["min_eligible"]:
            reasons.append(f"{name}:insufficient_eligible_candidates")
        if state.get("observation_gaps"):
            reasons.append(f"{name}:missing_session_observation_permanently_freezes_new_orders")
    targets, diagnostics = {name: {} for name in manifest["strategies"]}, {}
    if rebalance and any(allowed.values()):
        if not batch.get("snapshot_path"):
            raise ValueError("A verified current Qlib snapshot is required for close signal generation")
        targets, diagnostics = _candidate_targets(candidate, batch["snapshot_path"], session, eligible)
    previous_signal_day = calendar.previous_session(session)
    pending = state["signals"].get(previous_signal_day)
    if pending and pending["execution_session"] == session and session + "_open" not in state["completed"]:
        state["completed"][session + "_open"] = {"status": "expired_no_fill", "session": session,
            "reason": "Opening phase was missed; no retrospective fill", "recorded_at": clock.isoformat()}
        reasons.append("missed_open_expired_without_fill")
    signal = {"signal_date": session, "execution_session": next_session, "created_at": clock.isoformat(),
        "model_sha256": candidate.model_sha256, "model_manifest_sha256": candidate.manifest["manifest_sha256"],
        "snapshot_path": str(batch.get("snapshot_path")) if batch.get("snapshot_path") else None,
        "snapshot_manifest_sha256": snapshot_sha256,
        "universe": sorted(universe), "universe_evidence": current_evidence, "eligible": eligible,
        "exclusions": exclusions, "rebalance": rebalance, "allowed": allowed,
        "targets": targets, "diagnostics": diagnostics, "warnings": sorted(set(reasons))}
    signal["signal_sha256"] = _digest(signal)
    state["signals"][session] = signal
    state["current_universe"] = sorted(universe)
    state["valuations"] = [row for row in state["valuations"] if row["session"] != session]
    state["valuations"].append({"session": session, "observed_at": clock.isoformat(), "accounts": valuations})
    return {"status": "completed" if all(allowed.values()) else "partial", "phase": "close", "session": session,
            "execution_session": next_session, "rebalance": rebalance, "eligible_count": len(eligible),
            "exclusions": exclusions, "warnings": signal["warnings"], "signal_sha256": signal["signal_sha256"],
            "valuations": valuations, "corporate_actions": action_records}


def _open_operation(manifest, state, calendar, batch, clock):
    from stopat30m.backtest import ashare_execution as execution
    session = batch["session"]
    prior = calendar.previous_session(session)
    signal = state["signals"].get(prior)
    if (signal is None or signal["execution_session"] != session
            or signal["model_sha256"] != manifest["model_sha256"]
            or signal["model_manifest_sha256"] != manifest["candidate_manifest_sha256"]
            or signal["signal_sha256"] != _digest({key: value for key, value in signal.items() if key != "signal_sha256"})
            or _instant(datetime.fromisoformat(signal["created_at"])) >= datetime.combine(
                pd.Timestamp(session).date(), time(9, 30), SHANGHAI)):
        raise ValueError("Execution needs a preceding-session signal frozen before today's open")
    info, metadata_rejections = _security_metadata(batch["metadata"], session, clock)
    quotes, rejected = {}, dict(metadata_rejections)
    for instrument, row in batch["quotes"].items():
        try:
            stamp = _instant(datetime.fromisoformat(row.get("as_of", row.get("asof"))))
            fetched = _instant(datetime.fromisoformat(row.get("fetched_at", batch["fetched_at"])))
            if (row.get("source") != manifest["execution_source"] or row.get("session") != session
                    or stamp > clock or fetched > clock
                    or (clock - stamp).total_seconds() > manifest["max_quote_age_seconds"]
                    or (clock - fetched).total_seconds() > manifest["max_quote_age_seconds"]
                    or stamp.astimezone(SHANGHAI).date().isoformat() != session
                    or row.get("execution_price_basis") != "live_observed_quote"):
                raise ValueError("stale_or_nonlive_source_quote")
            quotes[instrument] = execution.OpeningQuote(session=session, open=row.get("open"), preclose=row.get("preclose"),
                tradestatus=_status(row.get("tradestatus")), source=row["source"],
                execution_price=row.get("execution_price", row.get("latest")),
                execution_price_basis="live_observed_quote", asof=stamp.isoformat())
        except (ValueError, KeyError, TypeError):
            rejected[instrument] = "stale_invalid_or_missing_live_quote"
    policy = execution.ExecutionPolicy(**manifest["execution_policy"])
    sessions = calendar.sessions(calendar.coverage_start, calendar.coverage_end)
    events = _action_book(state, batch, clock)
    requested = {key for rows in signal["targets"].values() for key in rows}
    if requested and not quotes and not all(
            _status(batch["quotes"].get(key, {}).get("tradestatus")) == "suspended"
            and batch["quotes"].get(key, {}).get("session") == session and key in info for key in requested):
        raise ValueError("No selected instrument has a current verified live execution quote")
    results = {}
    for name in manifest["strategies"]:
        account = execution.PortfolioState.from_dict(state["accounts"][name])
        account, action_records = _apply_actions(account, events, manifest=manifest,
            session=session, calendar=sessions, clock=clock)
        state["accounts"][name] = account.to_dict()
        if not signal["allowed"][name]:
            results[name] = {"status": "blocked_signal_no_fill", "orders": [], "trades": [],
                             "corporate_actions": action_records}
            continue
        # A changed exchange ex-right reference cannot silently lose cash/bonus
        # entitlements. An explicit corporate-action adapter is required first.
        for instrument, position in account.positions.items():
            quote = quotes.get(instrument)
            if (quote and quote.preclose is not None and position.last_mark is not None
                    and position.last_mark_date == prior
                    and not _verified_action_reference(account, instrument, session, quote.preclose, events)
                    and abs(execution.decimal(quote.preclose) - position.last_mark) >= Decimal("0.01")):
                raise ValueError(f"Unresolved held exchange reference/corporate action: {instrument}")
        result = execution.execute_target_weights(account, session=session, signal_date=prior,
            targets=signal["targets"][name], quotes=quotes, metadata=info, calendar=sessions,
            policy=policy, now=clock)
        state["accounts"][name] = result.state.to_dict()
        results[name] = {"status": "completed", "orders": result.orders, "trades": result.trades,
                         "opening_equity": str(result.opening_equity), "corporate_actions": action_records}
    return {"status": "completed", "phase": "open", "session": session,
            "signal_date": prior, "signal_sha256": signal["signal_sha256"], "accounts": results,
            "source_rejections": rejected, "live_order": False}


def run_ashare_forward(paper_dir, *, phase, provider, now=None):
    """One bounded phase. Provider methods are injected; this module has no HTTP.

    ``fetch_close(session,instruments,now)`` returns a JSON batch with current
    source raw close/metadata, immutable Qlib snapshot and actual evidence SHA.
    ``fetch_open`` returns current live observed prices, never a daily-bar replay.
    Closed sessions and non-rebalance opens never invoke either provider.
    """
    if phase not in {"open", "close"}:
        raise ValueError("A-share paper phase must be open or close")
    root = Path(paper_dir).resolve()
    with _lock(root):
        root, manifest, state, candidate, calendar = _read_paper(root)
        clock = _instant(now)
        local = clock.astimezone(SHANGHAI)
        session = local.date().isoformat()
        if not calendar.is_session(session):
            return {"status": "skipped_non_session", "session": session, "phase": phase, "network_called": False}
        if _record_observation_gaps(state, manifest, calendar, session, clock):
            _save_state(root, state)
        key = session + "_" + phase
        if key in state["completed"]:
            return {**state["completed"][key], "replayed": True, "network_called": False}
        if session < manifest["created_at"][:10] or clock < _instant(datetime.fromisoformat(manifest["created_at"])):
            raise ValueError("Paper cannot backfill dates before its initialization")
        if phase == "open" and not time(9, 30) <= local.time() <= time(9, 45):
            receipt = {"status": "expired_no_fill" if local.time() > time(9, 45) else "not_open_window",
                "session": session, "phase": phase, "recorded_at": clock.isoformat(), "network_called": False,
                "reason": "Only fresh observed prices within 09:30..09:45 authorize execution"}
            if local.time() > time(9, 45):
                state["completed"][key] = receipt
                _save_state(root, state)
            return receipt
        if phase == "close" and local.time() < time(15, 5):
            return {"status": "not_completed_session", "session": session, "phase": phase, "network_called": False}
        if phase == "open" and state.get("observation_gaps"):
            receipt = {"status": "blocked_observation_gap_no_fill", "session": session, "phase": phase,
                "recorded_at": clock.isoformat(), "network_called": False,
                "observation_gaps": state["observation_gaps"], "live_order": False}
            state["completed"][key] = receipt
            state["last_failure"] = receipt
            _save_state(root, state)
            return receipt
        prior = calendar.previous_session(session)
        signal = state["signals"].get(prior) if phase == "open" else None
        if phase == "open" and signal is None:
            return {"status": "no_frozen_previous_signal", "session": session, "phase": phase, "network_called": False}
        if phase == "open" and (not signal["rebalance"] or not any(signal["allowed"].values())):
            receipt = {"status": "skipped_non_rebalance" if not signal["rebalance"] else "blocked_signal_no_fill",
                "session": session, "phase": phase, "recorded_at": clock.isoformat(), "network_called": False}
            state["completed"][key] = receipt
            _save_state(root, state)
            return receipt
        attempted = state["attempts"].get(key, 0)
        if attempted >= manifest["daily_attempt_limit"]:
            return {"status": "attempt_limit", "session": session, "phase": phase, "network_called": False}
        attempt = attempted + 1
        state["attempts"][key] = attempt
        _save_state(root, state)  # survives a killed provider without altering ledgers
        working = json.loads(json.dumps(state))
        rights = set().union(*(_account_instruments(account) for account in state["accounts"].values()))
        needed = sorted(set(state["current_universe"]) | rights)
        if phase == "open":
            needed = sorted({key for rows in signal["targets"].values() for key in rows}
                            | rights)
        receipt_path = root / "receipts" / f"{key}_{attempt}.json"
        try:
            batch = getattr(provider, "fetch_" + phase)(session, needed, clock)
            clock = _instant(now)
            _batch(batch, phase, session, clock, manifest)
            if phase == "open" and not time(9, 30) <= clock.astimezone(SHANGHAI).time() <= time(9, 45):
                raise ValueError("Quote collection finished after the allowed live execution window")
            observation = root / "observations" / f"{key}_{attempt}.json"
            _write(observation, batch)
            if phase == "close":
                receipt = _close_operation(root, manifest, working, candidate, calendar, batch, clock)
            else:
                receipt = _open_operation(manifest, working, calendar, batch, clock)
            receipt.update({"attempt": attempt, "recorded_at": clock.isoformat(),
                "observation": str(observation), "observation_sha256": _sha(observation), "network_called": True})
            _write(receipt_path, receipt)
            if receipt["status"] == "completed":
                working["completed"][key] = {**receipt, "receipt_path": str(receipt_path), "receipt_sha256": _sha(receipt_path)}
            working["receipts"].append({"path": str(receipt_path), "sha256": _sha(receipt_path)})
            if receipt["status"] == "completed":
                working["last_success"] = {"session": session, "phase": phase, "recorded_at": clock.isoformat()}
                working["last_failure"] = None
            else:
                working["last_failure"] = {"status": "partial", "session": session, "phase": phase,
                    "warnings": receipt.get("warnings", []), "receipt_path": str(receipt_path),
                    "receipt_sha256": _sha(receipt_path), "recorded_at": clock.isoformat()}
            _save_state(root, working)
            return receipt
        except Exception as exc:
            receipt = {"status": "failed", "session": session, "phase": phase, "attempt": attempt,
                "recorded_at": _instant(now).isoformat(), "error_type": type(exc).__name__,
                "error": str(exc)[:500], "network_called": True, "live_order": False}
            _write(receipt_path, receipt)
            state["last_failure"] = {**receipt, "receipt_path": str(receipt_path), "receipt_sha256": _sha(receipt_path)}
            state["receipts"].append({"path": str(receipt_path), "sha256": _sha(receipt_path)})
            _save_state(root, state)
            return receipt


def ashare_forward_status(paper_dir):
    root, manifest, state, candidate, calendar = _read_paper(paper_dir)
    return {"path": str(root), "manifest": manifest, "state": state, "model_sha256": candidate.model_sha256,
        "calendar_source": calendar.source, "live_order_count": 0, "real_account_accessed": False}
