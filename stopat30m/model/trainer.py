"""
Model training pipeline.

Supports LightGBM, XGBoost, and PyTorch-based models via Qlib's model zoo.
Handles training, saving, loading, and incremental updates.
"""

from __future__ import annotations

import pickle
from dataclasses import asdict, dataclass
import hashlib
import json
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from stopat30m.config import get


@dataclass(frozen=True)
class FixedQlibLGBSpec:
    """One predeclared Alpha158 candidate, without tuning or early stopping."""
    num_boost_round: int = 100
    learning_rate: float = .03
    num_leaves: int = 31
    max_depth: int = 6
    min_data_in_leaf: int = 100
    lambda_l2: float = 1.0
    seed: int = 20261019

    def __post_init__(self):
        for key in ("num_boost_round", "num_leaves", "max_depth", "min_data_in_leaf", "seed"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{key} must be a positive integer")
        if self.num_leaves < 2 or self.num_leaves > 2 ** self.max_depth:
            raise ValueError("Leaf count exceeds the declared tree depth")
        for key in ("learning_rate", "lambda_l2"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not np.isfinite(value):
                raise ValueError("Invalid fixed learning rate/regularization")
        if not 0 < self.learning_rate <= 1 or self.lambda_l2 < 0:
            raise ValueError("Invalid fixed learning rate/regularization")

    def params(self):
        return {"objective": "regression_l2", "metric": "l2", "learning_rate": self.learning_rate,
            "num_leaves": self.num_leaves, "max_depth": self.max_depth,
            "min_data_in_leaf": self.min_data_in_leaf, "lambda_l2": self.lambda_l2,
            "seed": self.seed, "data_random_seed": self.seed, "feature_fraction_seed": self.seed,
            "bagging_seed": self.seed, "feature_fraction": 1.0, "bagging_fraction": 1.0,
            "bagging_freq": 0, "num_threads": 1, "deterministic": True, "force_col_wise": True,
            "verbosity": -1}


def _native_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def _native_sha(value):
    return hashlib.sha256(value).hexdigest()


class FixedQlibModel:
    """Native Booster with the Qlib dataset.predict interface; never pickle."""
    def __init__(self, booster, spec, feature_names, training_rows, metadata=None):
        self.booster, self.spec = booster, spec
        self.feature_names = list(feature_names)
        self.training_rows, self.metadata = training_rows, metadata or {}

    def predict(self, dataset, segment="test"):
        frame = dataset.prepare(segment, col_set="feature", data_key="infer")
        if isinstance(frame.columns, pd.MultiIndex):
            frame = frame["feature"]
        if list(frame.columns) != self.feature_names or self.booster.feature_name() != self.feature_names:
            raise ValueError("Qlib inference feature order differs from the frozen native model")
        values = frame.apply(pd.to_numeric, errors="raise")
        if np.isinf(values.to_numpy(dtype=float)).any():
            raise ValueError("Infinite Qlib inference features are not valid missing observations")
        scores = self.booster.predict(values, num_threads=1)
        if not np.isfinite(scores).all():
            raise ValueError("Native Qlib model returned nonfinite scores")
        return pd.Series(scores, index=frame.index, name="score")

    def save_native(self, path, *, metadata=None):
        import lightgbm as lgb
        target = Path(path)
        target.mkdir(parents=True, exist_ok=False)
        self.booster.save_model(str(target / "model.txt"))
        payload = {"schema_version": 1, "kind": "qlib_alpha158_fixed_lightgbm",
            "feature_names": self.feature_names, "training_rows": self.training_rows,
            "model_spec": asdict(self.spec), "train_params": self.spec.params(),
            "model_sha256": _native_sha((target / "model.txt").read_bytes()),
            "lightgbm_version": lgb.__version__, "numpy_version": np.__version__, "pandas_version": pd.__version__,
            "training_implementation_sha256": _native_sha(Path(__file__).read_bytes()),
            "early_stopping": False, "parameter_search": False, "metadata": metadata or self.metadata}
        payload["manifest_sha256"] = _native_sha(_native_json(payload))
        (target / "manifest.json").write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        return target


def fit_fixed_qlib_lightgbm(dataset, *, spec=None, expected_feature_count=158):
    """Fit only the purged train segment; validation/OOS are never fit inputs."""
    import lightgbm as lgb
    spec = spec or FixedQlibLGBSpec()
    frame = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    if not isinstance(frame.columns, pd.MultiIndex) or "feature" not in frame.columns.get_level_values(0) or "label" not in frame.columns.get_level_values(0):
        raise ValueError("Qlib learning data must expose feature and label column groups")
    features = frame["feature"].apply(pd.to_numeric, errors="raise")
    label = pd.to_numeric(frame["label"].iloc[:, 0], errors="raise")
    if len(features.columns) != expected_feature_count or features.columns.has_duplicates:
        raise ValueError("Qlib feature schema/count differs from the declared Alpha158 experiment")
    if np.isinf(features.to_numpy(dtype=float)).any() or np.isinf(label.to_numpy(dtype=float)).any():
        raise ValueError("Infinite training observations are invalid")
    complete = label.notna()
    features, label = features.loc[complete], label.loc[complete]
    if len(features) < max(60, 2 * spec.min_data_in_leaf):
        raise ValueError("Insufficient purged Qlib training observations")
    names = list(features.columns)
    train = lgb.Dataset(features, label=label, feature_name=names, free_raw_data=False)
    booster = lgb.train(spec.params(), train, num_boost_round=spec.num_boost_round)
    return FixedQlibModel(booster, spec, names, len(features))


def load_fixed_qlib_model(path):
    import lightgbm as lgb
    root = Path(path)
    payload = json.loads((root / "manifest.json").read_text())
    if payload.get("manifest_sha256") != _native_sha(_native_json({k: v for k, v in payload.items() if k != "manifest_sha256"})):
        raise ValueError("Native Qlib model manifest checksum mismatch")
    if payload.get("kind") != "qlib_alpha158_fixed_lightgbm" or payload.get("schema_version") != 1:
        raise ValueError("Unsupported native Qlib model bundle")
    if payload["model_sha256"] != _native_sha((root / "model.txt").read_bytes()):
        raise ValueError("Native Qlib model checksum mismatch")
    spec = FixedQlibLGBSpec(**payload["model_spec"])
    if payload["train_params"] != spec.params() or payload["lightgbm_version"] != lgb.__version__:
        raise ValueError("Native Qlib model parameters/runtime differ from the frozen declaration")
    booster = lgb.Booster(model_file=str(root / "model.txt"))
    if booster.feature_name() != payload["feature_names"]:
        raise ValueError("Native Qlib model feature order differs from its manifest")
    return FixedQlibModel(booster, spec, payload["feature_names"], payload["training_rows"], payload["metadata"])


class _PhaseCtx:
    """Mutable context for tracking sub-steps within a training phase."""

    __slots__ = ("total", "current", "desc")

    def __init__(self, total: int):
        self.total = total
        self.current = 0
        self.desc = ""

    def step(self, desc: str = "") -> None:
        """Mark the next sub-step as reached."""
        self.current += 1
        self.desc = desc


class TrainingProgress:
    """Phase-based progress monitor with sub-step tracking.

    Shows progress as "[phase X/N] step a/b description | pct% | ETA"
    so the user always sees meaningful changes between ticks.
    """

    PHASE_ORDER: list[tuple[str, float]] = [
        ("Building dataset", 0.60),
        ("Training model", 0.25),
        ("Generating predictions", 0.10),
        ("Evaluating", 0.05),
    ]

    def __init__(self, interval: float = 10.0):
        self._interval = interval
        self._global_start: float = 0
        self._phase_start: float = 0
        self._completed_weight: float = 0
        self._completed_time: float = 0
        self._current_phase: str = ""
        self._current_weight: float = 0
        self._phase_idx: int = 0
        self._total_phases: int = len(self.PHASE_ORDER)
        self._phase_weight_map = {name: w for name, w in self.PHASE_ORDER}
        self._phase_ctx: _PhaseCtx | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._global_start = time.monotonic()
        self._phase_idx = 0

    def finish(self) -> None:
        total = time.monotonic() - self._global_start
        logger.info(f"All done [{self._total_phases}/{self._total_phases}], total time: {_fmt_time(total)}")

    @contextmanager
    def phase(self, name: str, total_steps: int = 0):
        self._phase_idx += 1
        self._current_phase = name
        self._current_weight = self._phase_weight_map.get(name, 0.1)
        self._phase_start = time.monotonic()
        ctx = _PhaseCtx(total_steps)
        self._phase_ctx = ctx
        self._stop.clear()
        self._thread = threading.Thread(target=self._tick, daemon=True)
        self._thread.start()
        tag = f"[{self._phase_idx}/{self._total_phases}]"
        logger.info(f"▶ {tag} {name} ...")
        try:
            yield ctx
        finally:
            self._stop.set()
            elapsed = time.monotonic() - self._phase_start
            self._completed_time += elapsed
            self._completed_weight += self._current_weight
            if self._thread:
                self._thread.join(timeout=2)
            pct = int(self._completed_weight * 100)
            logger.info(f"  ✓ {tag} {name} done ({_fmt_time(elapsed)}) | overall {pct}%")

    def _tick(self) -> None:
        while not self._stop.wait(self._interval):
            phase_elapsed = time.monotonic() - self._phase_start
            tag = f"[{self._phase_idx}/{self._total_phases}]"
            ctx = self._phase_ctx

            parts = [f"  {tag} {self._current_phase}"]

            if ctx and ctx.total > 0:
                parts.append(f"| step {ctx.current}/{ctx.total}")
                if ctx.desc:
                    parts.append(ctx.desc)

            eta = self._estimate_eta(phase_elapsed)
            if eta is not None:
                in_progress_frac = self._estimate_phase_fraction(phase_elapsed)
                overall_pct = int((self._completed_weight + self._current_weight * in_progress_frac) * 100)
                parts.append(f"| {overall_pct}%")
                parts.append(f"| ETA ~{_fmt_time(eta)}")
            else:
                parts.append(f"| elapsed {_fmt_time(phase_elapsed)}")

            logger.info(" ".join(parts))

    def _estimate_phase_fraction(self, phase_elapsed: float) -> float:
        """Estimate how far through the current phase we are (0.0 ~ 1.0)."""
        ctx = self._phase_ctx
        if ctx and ctx.total > 0 and ctx.current > 0:
            return min(ctx.current / ctx.total, 0.99)
        if self._completed_weight > 0 and self._completed_time > 0:
            speed = self._completed_time / self._completed_weight
            expected = speed * self._current_weight
            if expected > 0:
                return min(phase_elapsed / expected, 0.99)
        return 0.0

    def _estimate_eta(self, phase_elapsed: float) -> float | None:
        if self._completed_weight <= 0:
            return None
        avg_speed = self._completed_time / self._completed_weight
        current_phase_estimate = avg_speed * self._current_weight
        current_remaining = max(0, current_phase_estimate - phase_elapsed)
        future_weight = 1.0 - self._completed_weight - self._current_weight
        future_estimate = avg_speed * max(0, future_weight)
        return current_remaining + future_estimate


def _fmt_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m >= 60:
        h, m = divmod(m, 60)
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"


class ModelTrainer:
    """Train and manage Qlib-compatible models."""

    SUPPORTED_MODELS = ("lgbm", "xgboost", "mlp", "lstm", "transformer")

    def __init__(
        self,
        model_type: str | None = None,
        model_params: dict[str, Any] | None = None,
        output_dir: str | None = None,
    ):
        cfg = get("model") or {}
        self.model_type = model_type or cfg.get("type", "lgbm")
        if self.model_type not in self.SUPPORTED_MODELS:
            raise ValueError(f"Unsupported model: {self.model_type}. Choose from {self.SUPPORTED_MODELS}")

        all_params = cfg.get("params", {})
        self.model_params = model_params or all_params.get(self.model_type, {})
        self.output_dir = Path(output_dir or cfg.get("output_dir", "./output/models"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.model: Any = None
        self._dataset: Any = None

    _XGB_FIT_KEYS = frozenset({"num_boost_round", "early_stopping_rounds"})

    def _create_model(self) -> Any:
        if self.model_type == "lgbm":
            from qlib.contrib.model.gbdt import LGBModel
            return LGBModel(**self.model_params)

        elif self.model_type == "xgboost":
            from qlib.contrib.model.xgboost import XGBModel
            init_params = {k: v for k, v in self.model_params.items() if k not in self._XGB_FIT_KEYS}
            return XGBModel(**init_params)

        elif self.model_type == "mlp":
            from qlib.contrib.model.pytorch_nn import DNNModel
            return DNNModel(**self.model_params)

        elif self.model_type == "lstm":
            from qlib.contrib.model.pytorch_lstm import LSTM
            return LSTM(**self.model_params)

        elif self.model_type == "transformer":
            from qlib.contrib.model.pytorch_transformer import Transformer
            return Transformer(**self.model_params)

        raise ValueError(f"Unknown model type: {self.model_type}")

    def _fit_kwargs(self) -> dict[str, Any]:
        """Extract model.fit() kwargs that are not __init__ params (e.g. XGBoost)."""
        if self.model_type == "xgboost":
            return {k: v for k, v in self.model_params.items() if k in self._XGB_FIT_KEYS}
        return {}

    def train(self, dataset: Any) -> Any:
        """Train model on dataset. Returns the trained model."""
        self._dataset = dataset
        self.model = self._create_model()

        logger.info(f"Training {self.model_type} model ...")
        self.model.fit(dataset, **self._fit_kwargs())
        logger.info("Training complete.")

        return self.model

    def predict(self, dataset: Any | None = None) -> Any:
        """Generate predictions."""
        if self.model is None:
            raise RuntimeError("Model not trained. Call train() or load() first.")

        ds = dataset or self._dataset
        if ds is None:
            raise RuntimeError("No dataset provided.")

        logger.info("Generating predictions ...")
        pred = self.model.predict(ds)
        logger.info(f"Predictions shape: {pred.shape}")
        return pred

    def save(self, name: str = "model") -> Path:
        """Persist model to disk."""
        if self.model is None:
            raise RuntimeError("No model to save.")

        path = self.output_dir / f"{name}_{self.model_type}.pkl"
        with open(path, "wb") as f:
            pickle.dump(self.model, f)
        logger.info(f"Model saved to {path}")
        return path

    def load(self, path: str | Path) -> Any:
        """Load a persisted model."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Model file not found: {path}")

        with open(path, "rb") as f:
            self.model = pickle.load(f)
        logger.info(f"Model loaded from {path}")
        return self.model


def train_and_evaluate(
    dataset: Any,
    model_type: str | None = None,
    save_name: str = "model",
    progress: TrainingProgress | None = None,
) -> dict[str, Any]:
    """
    Convenience: train model, evaluate, save, return results.

    Returns dict with keys: model, predictions, metrics, model_path
    """
    from stopat30m.model.evaluator import evaluate_predictions

    trainer = ModelTrainer(model_type=model_type)

    if progress:
        with progress.phase("Training model"):
            model = trainer.train(dataset)
        with progress.phase("Generating predictions"):
            pred = trainer.predict(dataset)
    else:
        model = trainer.train(dataset)
        pred = trainer.predict(dataset)

    model_path = trainer.save(save_name)

    if progress:
        with progress.phase("Evaluating"):
            metrics = evaluate_predictions(pred, dataset)
    else:
        metrics = evaluate_predictions(pred, dataset)

    return {
        "model": model,
        "predictions": pred,
        "metrics": metrics,
        "model_path": model_path,
    }
