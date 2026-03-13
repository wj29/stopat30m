"""
Model training pipeline.

Supports LightGBM, XGBoost, and PyTorch-based models via Qlib's model zoo.
Handles training, saving, loading, and incremental updates.
"""

from __future__ import annotations

import pickle
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from loguru import logger

from stopat30m.config import get


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
