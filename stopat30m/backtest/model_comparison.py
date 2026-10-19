"""One frozen LightGBM candidate for the locked, small fixed-ETF experiment.

Features are close-time causal, labels enter at the next session's open and
exit after H open-to-open intervals. Only labels ending within development are
fitted; validation and final OOS never select parameters or stopping rounds.
The saved native Booster is reused by future simulations without refitting.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import tempfile
from typing import Any

import numpy as np
import pandas as pd

from . import walk_forward as wf

MODEL_POLICY_VERSION = "fixed-etf-lightgbm-open20-v1"
FEATURE_VERSION = "causal-ohlcv-ratios-v1"
FEATURE_NAMES = (
    "return_1", "return_5", "return_20", "return_60", "volatility_20", "volatility_60",
    "intraday_return", "overnight_gap", "daily_range", "range_mean_20",
    "volume_ratio_20", "close_to_ma20", "close_to_ma60",
)
FEATURE_WARMUP = 60
LIMITATIONS = [
    "仅在声明的小型固定ETF池进行回归排序/配置实验，不能据此证明A股个股选股有效。",
    "模型实际训练与协议锁定时间均为本次执行时间；2018截止训练是回溯隔离设计，不是假称当年已经训练或预注册。",
    "开发期标签高度重叠且同日ETF相关，训练样本数不等于独立观测数；没有参数搜索、验证集早停或最终样本外重训。",
    "模型标签是复权零碎敞口的次日开盘至20个交易日后开盘、扣固定双边成本收益，不是券商整数股可执行成交保证。",
    "特征仅用信号日收盘及之前OHLCV，不包含新闻、大语言模型结论、证券身份编码或未来价格；不填补缺价。",
    "原始成交量仅用于同证券滚动相对量，未证明所有公司行为或历史数据修订已按当时版本还原。",
    "模型、协议与依赖版本冻结；未来模拟只生成新预测，不在线学习或根据未来结果自动更换模型。",
]


@dataclass(frozen=True)
class FrozenModelSpec:
    label_horizon_sessions: int = 20
    num_boost_round: int = 100
    learning_rate: float = .03
    num_leaves: int = 7
    max_depth: int = 3
    min_data_in_leaf: int = 50
    lambda_l2: float = 1.0
    seed: int = 20261019

    def __post_init__(self):
        for name in ("label_horizon_sessions", "num_boost_round", "num_leaves", "max_depth", "min_data_in_leaf", "seed"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.num_leaves < 2 or self.num_leaves > 2 ** self.max_depth:
            raise ValueError("num_leaves must be within the declared finite tree depth")
        for name in ("learning_rate", "lambda_l2"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if not 0 < self.learning_rate <= 1 or self.lambda_l2 < 0:
            raise ValueError("Invalid frozen learning rate or regularization")

    def as_dict(self):
        return asdict(self)

    def train_params(self):
        return {"objective": "regression_l2", "metric": "l2", "learning_rate": self.learning_rate,
                "num_leaves": self.num_leaves, "max_depth": self.max_depth,
                "min_data_in_leaf": self.min_data_in_leaf, "lambda_l2": self.lambda_l2,
                "seed": self.seed, "data_random_seed": self.seed, "feature_fraction_seed": self.seed,
                "bagging_seed": self.seed, "feature_fraction": 1.0, "bagging_fraction": 1.0,
                "bagging_freq": 0, "num_threads": 1, "deterministic": True,
                "force_col_wise": True, "verbosity": -1}


def _bytes(value):
    return wf._canonical(value)


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _json_write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                                    allow_nan=False, default=wf._default_json) + "\n", encoding="utf-8")


def _code_hash():
    return _sha(Path(__file__).read_bytes())


def _validate_protocol(payload):
    if not isinstance(payload, dict) or payload.get("schema_version") != 1 or payload.get("policy_version") != MODEL_POLICY_VERSION:
        raise ValueError("Unsupported frozen model protocol")
    if payload.get("protocol_sha256") != _sha(_bytes({k: v for k, v in payload.items() if k != "protocol_sha256"})):
        raise ValueError("Frozen model protocol checksum mismatch")
    spec = FrozenModelSpec(**payload["model_spec"])
    if payload.get("train_params") != spec.train_params() or payload.get("feature_warmup_sessions") != FEATURE_WARMUP:
        raise ValueError("Frozen model parameter or warmup declaration is inconsistent")
    base = payload["base_protocol"]
    if base.get("protocol_sha256") != _sha(_bytes({k: v for k, v in base.items() if k != "protocol_sha256"})):
        raise ValueError("Base research protocol checksum mismatch")
    if payload.get("base_protocol_sha256") != base["protocol_sha256"] or base.get("policy_version") != wf.POLICY_VERSION:
        raise ValueError("Frozen model is not bound to the declared baseline protocol")
    if base.get("universe_sha256") != wf._digest(base.get("universe")) or base["universe"].get("kind") != "fixed":
        raise ValueError("This model experiment requires the unchanged explicit fixed ETF universe")
    cfg = wf.ResearchConfig(**base["config"])
    if cfg.top_k != 1 or cfg.rebalance_sessions != spec.label_horizon_sessions:
        raise ValueError("Frozen model requires top1 and label horizon equal to the baseline rebalance interval")
    if (payload.get("feature_version") != FEATURE_VERSION or payload.get("feature_names") != list(FEATURE_NAMES)
            or payload.get("feature_implementation_sha256") != _code_hash()):
        raise ValueError("Frozen feature schema/order does not match this implementation")
    return spec, cfg


def lock_model_protocol(base_protocol_path, path, *, spec: FrozenModelSpec | None = None):
    """Create a separate immutable model lock; never rewrite the baseline lock."""
    spec = spec or FrozenModelSpec()
    base = wf.load_protocol(base_protocol_path)
    binding = {"schema_version": 1, "policy_version": MODEL_POLICY_VERSION,
               "base_protocol": base, "base_protocol_sha256": base["protocol_sha256"],
               "model_spec": spec.as_dict(), "train_params": spec.train_params(),
               "feature_version": FEATURE_VERSION, "feature_names": list(FEATURE_NAMES),
               "feature_implementation_sha256": _code_hash(),
               "feature_warmup_sessions": FEATURE_WARMUP,
               "label": "adjusted_open[t+H+1]*(1-sell_cost)/(adjusted_open[t+1]*(1+buy_cost))-1",
               "model_selection": "one fixed candidate; no early stopping; validation diagnostic only; no OOS fitting",
               "selection_rule": "top1 predicted net holding return strictly >0, else cash; instrument ascending tie break",
               "limitations": LIMITATIONS}
    target = Path(path)
    if target.exists():
        current = json.loads(target.read_text(encoding="utf-8"))
        _validate_protocol(current)
        if any(current.get(key) != value for key, value in binding.items()):
            raise ValueError("Frozen model protocol already exists with different declarations")
        return current
    payload = {**binding, "locked_at": datetime.now(timezone.utc).isoformat()}
    payload["protocol_sha256"] = _sha(_bytes(payload))
    _validate_protocol(payload)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")
    return payload


def _load_protocol(path):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    spec, cfg = _validate_protocol(payload)
    return payload, spec, cfg


def _inputs(artifact, instruments, end_date=None):
    calendar = wf._dates(artifact.calendar, "Model exchange calendar")
    if end_date is not None:
        calendar = calendar[calendar <= pd.Timestamp(end_date)]
    if calendar.empty:
        raise ValueError("No declared exchange sessions before the feature cutoff")
    fields = ["adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close", "raw_volume"]
    source = getattr(artifact, "prices", None)
    if not isinstance(source, pd.DataFrame) or not {"date", "instrument", *fields}.issubset(source.columns):
        raise ValueError("Model features require long-form adjusted OHLC and raw share-volume")
    # Slice before numeric conversion or feature/label construction. Training
    # never examines values or outcomes beyond the development cutoff.
    source = source.loc[source["instrument"].isin(instruments)].copy()
    source["date"] = pd.to_datetime(source["date"], errors="raise")
    source = source.loc[source["date"] <= calendar[-1]]
    if source.duplicated(["date", "instrument"]).any() or not source.date.isin(calendar).all():
        raise ValueError("Model bars must be unique and belong to the source exchange calendar")
    if set(source.instrument.unique()) != set(instruments):
        raise ValueError("Model artifact is missing a frozen pool instrument")
    frames = {}
    for field in fields:
        source[field] = pd.to_numeric(source[field], errors="raise").astype(float)
        values = source[field].dropna().to_numpy()
        if not np.isfinite(values).all() or (values < 0 if field == "raw_volume" else values <= 0).any():
            raise ValueError(f"Invalid model feature input: {field}")
        frames[field] = source.pivot(index="date", columns="instrument", values=field).reindex(index=calendar, columns=instruments)
    opens, high, low, close = [frames[f"adjusted_{field}"] for field in ("open", "high", "low", "close")]
    tolerance = 1e-9
    if ((high + tolerance < np.maximum(opens, close)) | (low - tolerance > np.minimum(opens, close)) | (high < low)).any().any():
        raise ValueError("Adjusted OHLC range is inconsistent")
    return calendar, frames, source


def build_features(artifact, instruments, *, end_date=None):
    """Causal daily features, including explicit NaNs for missing/warmup bars.

    No scaling/imputation is fitted across dates. Reindexing uses the supplied
    exchange calendar; a missing bar invalidates its full trailing window.
    """
    instruments = sorted(set(instruments))
    calendar, frames, _ = _inputs(artifact, instruments, end_date)
    rows = []
    for inst in instruments:
        op, hi, lo, cl, vol = [frames[field][inst] for field in ("adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close", "raw_volume")]
        feature = pd.DataFrame(index=calendar)
        for window in (1, 5, 20, 60):
            feature[f"return_{window}"] = cl.pct_change(window, fill_method=None)
        daily_return = cl.pct_change(fill_method=None)
        for window in (20, 60):
            feature[f"volatility_{window}"] = daily_return.rolling(window, min_periods=window).std(ddof=0)
        feature["intraday_return"] = cl / op - 1
        feature["overnight_gap"] = op / cl.shift(1) - 1
        feature["daily_range"] = (hi - lo) / cl
        feature["range_mean_20"] = feature.daily_range.rolling(20, min_periods=20).mean()
        feature["volume_ratio_20"] = vol / vol.rolling(20, min_periods=20).mean().replace(0, np.nan)
        feature["close_to_ma20"] = cl / cl.rolling(20, min_periods=20).mean() - 1
        feature["close_to_ma60"] = cl / cl.rolling(60, min_periods=60).mean() - 1
        complete = pd.concat([op, hi, lo, cl, vol], axis=1).notna().all(axis=1).rolling(FEATURE_WARMUP + 1, min_periods=FEATURE_WARMUP + 1).min().eq(1)
        feature = feature.loc[:, FEATURE_NAMES].replace([np.inf, -np.inf], np.nan)
        feature.loc[~complete, :] = np.nan
        feature.insert(0, "instrument", inst)
        feature.insert(0, "signal_date", calendar)
        rows.append(feature.reset_index(drop=True))
    return pd.concat(rows, ignore_index=True).sort_values(["signal_date", "instrument"]).reset_index(drop=True)


def _labels(artifact, instruments, spec, cfg, *, end_date=None):
    calendar, frames, source = _inputs(artifact, instruments, end_date)
    buy = (cfg.buy_fee_bps + cfg.slippage_bps) / 10000
    sell = (cfg.sell_fee_bps + cfg.slippage_bps) / 10000
    horizon, rows = spec.label_horizon_sessions, []
    for inst in instruments:
        op, cl, vol = [frames[field][inst] for field in ("adjusted_open", "adjusted_close", "raw_volume")]
        entry, exit_ = op.shift(-1), op.shift(-(horizon + 1))
        valid = op.notna() & cl.notna()
        holding_covered = valid.rolling(horizon + 1, min_periods=horizon + 1).min().shift(-(horizon + 1)).eq(1)
        tradable = pd.Series(True, index=calendar)
        if "tradestatus" in source:
            status = source.loc[source.instrument.eq(inst)].set_index("date").tradestatus.reindex(calendar)
            tradable &= ~status.map(wf._suspended)
        tradable &= vol.gt(0)
        label = exit_ * (1 - sell) / (entry * (1 + buy)) - 1
        valid_label = holding_covered & tradable.shift(-1, fill_value=False) & tradable.shift(-(horizon + 1), fill_value=False)
        label = label.where(valid_label)
        dates = pd.Series(calendar, index=calendar)
        rows.append(pd.DataFrame({"signal_date": calendar, "instrument": inst,
                                  "label_entry_date": dates.shift(-1).to_numpy(),
                                  "label_exit_date": dates.shift(-(horizon + 1)).to_numpy(),
                                  "label": label.to_numpy()}))
    return pd.concat(rows, ignore_index=True).sort_values(["signal_date", "instrument"]).reset_index(drop=True)


def _lgbm():
    try:
        import lightgbm
    except (ImportError, OSError):
        raise RuntimeError("Frozen ML research requires LightGBM >=4.6,<5 and its platform OpenMP runtime") from None
    parts = tuple(int(part) for part in lightgbm.__version__.split(".")[:2])
    if not (parts >= (4, 6) and parts < (5, 0)):
        raise RuntimeError("Frozen ML research requires LightGBM >=4.6,<5")
    return lightgbm


def _environment(lgb):
    return {"python": platform.python_version(), "lightgbm": lgb.__version__, "pandas": pd.__version__,
            "numpy": np.__version__, "platform": platform.system(), "machine": platform.machine()}


def _split(day, cfg):
    text = pd.Timestamp(day).date().isoformat()
    for name, start, end in (("train", cfg.train_start, cfg.train_end), ("validation", cfg.validation_start, cfg.validation_end),
                             ("final_oos", cfg.oos_start, cfg.oos_end)):
        if start <= text <= end:
            return name
    return "outside_evaluation"


def _predict(booster, features, cfg, model_sha):
    result = features[["signal_date", "instrument"]].copy()
    complete = np.isfinite(features.loc[:, FEATURE_NAMES].to_numpy()).all(axis=1)
    result["score"] = np.nan
    if complete.any():
        scores = np.asarray(booster.predict(features.loc[complete, FEATURE_NAMES], num_threads=1), dtype=float)
        if len(scores) != int(complete.sum()) or not np.isfinite(scores).all():
            raise ValueError("Frozen model returned invalid prediction scores")
        result.loc[complete, "score"] = scores
    result["status"] = np.where(complete, "available", "missing_features")
    result["split"] = result.signal_date.map(lambda day: _split(day, cfg))
    result["training_period_in_sample"] = result["split"].eq("train")
    result["model_sha256"] = model_sha
    return result


def _csv(path, frame):
    frame.to_csv(path, index=False, date_format="%Y-%m-%d", float_format="%.17g", na_rep="")


def _diagnostics(predictions, labels, cfg):
    joined = predictions.merge(labels, on=["signal_date", "instrument"], validate="one_to_one")
    result = {}
    for name, end in (("train", cfg.train_end), ("validation", cfg.validation_end), ("final_oos", cfg.oos_end)):
        part = joined.loc[joined["split"].eq(name) & joined.label_exit_date.le(pd.Timestamp(end))].dropna(subset=["score", "label"])
        error = part.score - part.label
        correlation = part[["score", "label"]].corr().iloc[0, 1] if len(part) > 1 else np.nan
        result[name] = {"rows": len(part), "rmse": float(np.sqrt(np.mean(error ** 2))) if len(part) else None,
                        "pooled_correlation": float(correlation) if np.isfinite(correlation) else None,
                        "used_for_fit": name == "train", "used_for_model_selection": False,
                        "note": "Overlapping 20-session labels and correlated ETFs are not independent observations."}
    return result


def fit_frozen_model(artifact, universe, model_protocol_path, output_dir):
    """Fit once on purged development labels; publish an immutable native model."""
    protocol, spec, cfg = _load_protocol(model_protocol_path)
    base = protocol["base_protocol"]
    pool = wf._universe_payload(universe)
    if wf._digest(pool) != base["universe_sha256"]:
        raise ValueError("Training pool differs from the locked ETF universe")
    audit = wf._audit_universe(universe, cfg, pool)
    source_manifest = wf._validate_source(artifact, pool, cfg)
    instruments = sorted({row["instrument"] for row in pool["intervals"]})
    target = Path(output_dir)
    if target.exists():
        raise FileExistsError("Frozen model output already exists; loading never retrains or overwrites")
    # The training construction is physically bounded before any OOS values.
    features = build_features(artifact, instruments, end_date=cfg.train_end)
    labels = _labels(artifact, instruments, spec, cfg, end_date=cfg.train_end)
    dataset = features.merge(labels, on=["signal_date", "instrument"], validate="one_to_one")
    in_period = dataset.signal_date.between(pd.Timestamp(cfg.train_start), pd.Timestamp(cfg.train_end))
    label_covered = dataset.label_exit_date.le(pd.Timestamp(cfg.train_end)) & dataset.label.notna()
    complete = np.isfinite(dataset.loc[:, FEATURE_NAMES].to_numpy()).all(axis=1)
    eligible_cache = {}
    for day, entry in dataset.loc[in_period & label_covered, ["signal_date", "label_entry_date"]].drop_duplicates().itertuples(index=False):
        eligible_cache[day] = set(wf._members(universe, day, day)) & set(wf._members(universe, entry, day))
    eligible = pd.Series([row.instrument in eligible_cache.get(row.signal_date, set()) for row in dataset.itertuples()], index=dataset.index)
    training = dataset.loc[in_period & label_covered & complete & eligible].copy().reset_index(drop=True)
    if len(training) < max(60, 2 * spec.min_data_in_leaf):
        raise ValueError("Insufficient purged, complete development observations for the declared model")
    lgb = _lgbm()
    train_set = lgb.Dataset(training.loc[:, FEATURE_NAMES], label=training.label,
                            feature_name=list(FEATURE_NAMES), free_raw_data=False)
    booster = lgb.train(spec.train_params(), train_set, num_boost_round=spec.num_boost_round)
    if booster.feature_name() != list(FEATURE_NAMES):
        raise ValueError("LightGBM feature order differs from the locked feature schema")
    # OOS features/outcomes are accessed only after all model fitting is over.
    all_features = build_features(artifact, instruments)
    all_labels = _labels(artifact, instruments, spec, cfg)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".frozen-ml-", dir=target.parent) as temporary:
        stage = Path(temporary)
        booster.save_model(str(stage / "model.txt"))
        model_sha = _sha((stage / "model.txt").read_bytes())
        predictions = _predict(booster, all_features, cfg, model_sha)
        _csv(stage / "training.csv", training)
        _csv(stage / "features.csv", all_features)
        _csv(stage / "predictions.csv", predictions)
        _csv(stage / "diagnostic_labels.csv", all_labels)
        _json_write(stage / "diagnostics.json", _diagnostics(predictions, all_labels, cfg))
        _json_write(stage / "protocol.json", protocol)
        files = {path.name: _sha(path.read_bytes()) for path in sorted(stage.iterdir())}
        manifest = {"schema_version": 1, "policy_version": MODEL_POLICY_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(), "trained_at": datetime.now(timezone.utc).isoformat(),
            "model_sha256": model_sha, "protocol_sha256": protocol["protocol_sha256"],
            "base_protocol_sha256": base["protocol_sha256"], "universe_sha256": base["universe_sha256"],
            "instruments": instruments, "feature_version": FEATURE_VERSION, "feature_names": list(FEATURE_NAMES),
            "feature_implementation_sha256": _code_hash(), "environment": _environment(lgb),
            "train_params": spec.train_params(), "model_spec": spec.as_dict(),
            "training": {"rows": len(training), "first_signal_date": str(training.signal_date.min().date()),
                "last_signal_date": str(training.signal_date.max().date()), "last_label_exit_date": str(training.label_exit_date.max().date()),
                "declared_cutoff": cfg.train_end, "period_candidate_rows": int(in_period.sum()),
                "purged_or_missing_label_rows": int((in_period & ~label_covered).sum()),
                "excluded_missing_feature_rows": int((in_period & label_covered & ~complete).sum()),
                "training_csv_sha256": files["training.csv"], "validation_used_for_fit": False,
                "final_oos_used_for_fit": False, "early_stopping": False, "automatic_parameter_search": False},
            "source_snapshot": str(getattr(artifact, "path", "")), "source_manifest": source_manifest,
            "source_manifest_sha256": getattr(artifact, "manifest_sha256", None), "universe_audit": audit,
            "files": files, "execution_ready": False, "limitations": LIMITATIONS}
        manifest["manifest_sha256"] = _sha(_bytes(manifest))
        _json_write(stage / "manifest.json", manifest)
        target.mkdir(exist_ok=False)
        # Publish the manifest last so a partial write is never a loadable model.
        for path in sorted(stage.iterdir(), key=lambda path: path.name == "manifest.json"):
            path.replace(target / path.name)
    return load_frozen_model(target)


@dataclass
class FrozenModelArtifact:
    path: Path
    manifest: dict[str, Any]
    booster: Any

    @property
    def model_sha256(self):
        return self.manifest["model_sha256"]

    def _verify(self):
        _verify_artifact(self.path, expected=self.manifest)
        if self.booster.feature_name() != list(FEATURE_NAMES):
            raise ValueError("Loaded model feature order changed")

    def predict(self, artifact, signal_dates=None):
        """Score only history up to requested signal dates; never fit again.

        New future snapshots may extend beyond historical OOS. They must retain
        the same provider and adjustment convention; the caller records the new
        snapshot identity and supplies its real exchange calendar.
        """
        self._verify()
        old = self.manifest["source_manifest"]
        current = getattr(artifact, "manifest", {})
        if current.get("provider", {}).get("name") != old.get("provider", {}).get("name"):
            raise ValueError("Prediction provider differs from the frozen training source")
        if current.get("request", {}).get("adjustment") != old.get("request", {}).get("adjustment"):
            raise ValueError("Prediction adjustment differs from the frozen model")
        source_path = Path(getattr(artifact, "path", "."))
        for name, record in current.get("files", {}).items():
            file = source_path / name
            if Path(name).name != name or not file.is_file() or _sha(file.read_bytes()) != record.get("sha256"):
                raise ValueError("Prediction source snapshot checksum mismatch")
        if getattr(artifact, "manifest_sha256", None):
            raw = (source_path / "manifest.json").read_bytes()
            if _sha(raw) != artifact.manifest_sha256 or json.loads(raw) != current:
                raise ValueError("Prediction source manifest changed after loading")
        dates = wf._dates(signal_dates, "Prediction signal") if signal_dates is not None else None
        features = build_features(artifact, self.manifest["instruments"], end_date=dates[-1] if dates is not None else None)
        if dates is not None:
            available = pd.DatetimeIndex(features.signal_date.unique())
            if not dates.isin(available).all():
                raise ValueError("Prediction date is absent from the source exchange calendar")
            features = features.loc[features.signal_date.isin(dates)].copy()
        _, _, cfg = _load_protocol(self.path / "protocol.json")
        return _predict(self.booster, features, cfg, self.model_sha256)

    def target_weights(self, predictions, signal_date, execution_date, eligible_instruments, cash_reserve_pct=0):
        """Return (weights, signal_rows) for the shared accounting callback."""
        self._verify()
        if not isinstance(predictions, pd.DataFrame) or not {"signal_date", "instrument", "score", "model_sha256"}.issubset(predictions):
            raise ValueError("Frozen predictions require dates, instruments, scores and model identity")
        signal, execution = pd.Timestamp(signal_date), pd.Timestamp(execution_date)
        if signal >= execution:
            raise ValueError("Model signal must strictly precede its open-session execution")
        if isinstance(cash_reserve_pct, bool) or not math.isfinite(cash_reserve_pct) or not 0 <= cash_reserve_pct < 1:
            raise ValueError("Invalid cash reserve")
        members = sorted(set(eligible_instruments))
        if not set(members).issubset(self.manifest["instruments"]):
            raise ValueError("Prediction target is outside the frozen ETF pool")
        if not members:
            raise ValueError("No eligible frozen ETF exposures at execution")
        current = predictions.loc[pd.to_datetime(predictions.signal_date).eq(signal) & predictions.instrument.isin(members)].copy()
        if current.instrument.duplicated().any() or set(current.instrument) != set(members):
            raise ValueError("Missing or duplicate required model predictions")
        if not current.model_sha256.eq(self.model_sha256).all():
            raise ValueError("Prediction model identity differs from the frozen model")
        values = pd.to_numeric(current.score, errors="raise").to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError("Missing model features/predictions; cannot silently substitute cash or another ETF")
        ranked = sorted(zip(current.instrument, values), key=lambda pair: (-pair[1], pair[0]))
        selected = ranked[0][0] if ranked[0][1] > 0 else None
        rows = [{"date": execution, "signal_date": signal, "instrument": inst, "score": float(score),
                 "reason": "eligible" if score > 0 else "nonpositive_predicted_return", "selected": inst == selected,
                 "model_sha256": self.model_sha256} for inst, score in ranked]
        return ({selected: 1 - cash_reserve_pct} if selected else {}), rows


def _verify_artifact(path, expected=None):
    root = Path(path)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("manifest_sha256") != _sha(_bytes({k: v for k, v in manifest.items() if k != "manifest_sha256"})):
        raise ValueError("Frozen model manifest checksum mismatch")
    if expected is not None and manifest != expected:
        raise ValueError("Frozen model manifest changed after loading")
    if manifest.get("schema_version") != 1 or manifest.get("policy_version") != MODEL_POLICY_VERSION or manifest.get("feature_version") != FEATURE_VERSION or manifest.get("feature_names") != list(FEATURE_NAMES):
        raise ValueError("Frozen model feature schema/order mismatch")
    if manifest.get("feature_implementation_sha256") != _code_hash():
        raise ValueError("Feature implementation changed; an existing frozen model cannot silently use new code")
    required = {"model.txt", "protocol.json", "training.csv", "features.csv", "predictions.csv", "diagnostic_labels.csv", "diagnostics.json"}
    if set(manifest.get("files", {})) != required:
        raise ValueError("Frozen model artifact has incomplete file coverage")
    for name, expected_sha in manifest["files"].items():
        file = root / name
        if Path(name).name != name or file.is_symlink() or not file.is_file() or _sha(file.read_bytes()) != expected_sha:
            raise ValueError(f"Frozen model artifact checksum mismatch: {name}")
    if manifest["model_sha256"] != manifest["files"]["model.txt"]:
        raise ValueError("Frozen model identity is inconsistent")
    protocol, spec, _ = _load_protocol(root / "protocol.json")
    if protocol["protocol_sha256"] != manifest["protocol_sha256"] or protocol["base_protocol_sha256"] != manifest["base_protocol_sha256"]:
        raise ValueError("Frozen model protocol binding mismatch")
    pool = protocol["base_protocol"]["universe"]
    if (manifest.get("universe_sha256") != protocol["base_protocol"]["universe_sha256"]
            or manifest.get("instruments") != sorted({row["instrument"] for row in pool["intervals"]})
            or manifest.get("model_spec") != spec.as_dict() or manifest.get("train_params") != spec.train_params()):
        raise ValueError("Frozen model declarations differ from the bound protocol")
    return manifest


def load_frozen_model(path):
    root = Path(path)
    manifest = _verify_artifact(root)
    lgb = _lgbm()
    environment = _environment(lgb)
    for name in ("lightgbm", "pandas", "numpy"):
        if environment[name] != manifest["environment"][name]:
            raise ValueError(f"Use the recorded exact {name} version to load this frozen model")
    booster = lgb.Booster(model_file=str(root / "model.txt"))
    if booster.feature_name() != list(FEATURE_NAMES):
        raise ValueError("Native Booster feature order differs from the frozen schema")
    return FrozenModelArtifact(root, manifest, booster)
