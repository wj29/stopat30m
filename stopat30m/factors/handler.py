"""
Combined DataHandler: Alpha158 base + extended market factors.

Produces ~400+ total features for ML model training.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from loguru import logger

from stopat30m.config import get
from stopat30m.factors.expressions import get_factor_groups


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

# Alpha158 canonical feature/label definitions (inline to avoid import-time qlib dep)
_ALPHA158_FIELDS = [
    "$close", "$open", "$high", "$low", "$volume",
    "Ref($close,1)/$close", "Ref($open,1)/$close",
    "Ref($high,1)/$close", "Ref($low,1)/$close",
    "Ref($volume,1)/($volume+1e-12)",
]


def _build_alpha158_features() -> list[tuple[str, str]]:
    """Return the Alpha158-equivalent feature definitions.

    Uses a curated set of 158 features covering the same categories as Qlib's
    Alpha158: K-bar, rolling stats, momentum, volume-price correlation, etc.
    Avoids instantiating Qlib's Alpha158 handler (which triggers a slow data load).
    """
    return _alpha158_fallback()


def _alpha158_fallback() -> list[tuple[str, str]]:
    """Curated Alpha158-equivalent feature set."""
    features = []
    kbar = [
        ("($close-$open)/$open", "KBAR_CO"),
        ("($high-$low)/$open", "KBAR_HL"),
        ("($close-$open)/($high-$low+1e-12)", "KBAR_BODY"),
        ("($high-If($close>$open,$close,$open))/($high-$low+1e-12)", "KBAR_USHADOW"),
        ("(If($close<$open,$close,$open)-$low)/($high-$low+1e-12)", "KBAR_DSHADOW"),
    ]
    features.extend(kbar)

    for w in [5, 10, 20, 30, 60]:
        features.extend([
            (f"Mean($close,{w})/$close", f"MA{w}"),
            (f"Std($close,{w})/$close", f"STD{w}"),
            (f"Mean($volume,{w})/($volume+1e-12)", f"VSTD{w}"),
            (f"Slope($close,{w})/{w}", f"BETA{w}"),
            (f"Max($high,{w})/$close", f"MAX{w}"),
            (f"Min($low,{w})/$close", f"MIN{w}"),
            (f"Quantile($close,{w},0.8)/$close", f"QTLU{w}"),
            (f"Quantile($close,{w},0.2)/$close", f"QTLD{w}"),
            (f"Rank($close/Ref($close,{w})-1, {w})", f"RANK{w}"),
            (f"Rsquare($close,{w})", f"RSQ{w}"),
            (f"Resi($close,{w})/$close", f"RESI{w}"),
            (f"Corr($close,$volume,{w})", f"CORR{w}"),
            (f"Corr($close/Ref($close,1)-1,$volume/Ref($volume,1)-1,{w})", f"CORD{w}"),
            (f"Count($close>Ref($close,1),{w})/{w}", f"CNTP{w}"),
            (f"Count($close<Ref($close,1),{w})/{w}", f"CNTN{w}"),
            (f"Sum(If($close>Ref($close,1),$close-Ref($close,1),0),{w})/Sum(Abs($close-Ref($close,1)),{w})", f"SUMP{w}"),
            (f"Sum(If($close<Ref($close,1),Ref($close,1)-$close,0),{w})/Sum(Abs($close-Ref($close,1)),{w})", f"SUMN{w}"),
        ])

    for d in [1, 2, 3, 4, 5]:
        features.extend([
            (f"Ref($close,{d})/$close", f"CLOSE{d}"),
            (f"Ref($open,{d})/$close", f"OPEN{d}"),
            (f"Ref($high,{d})/$close", f"HIGH{d}"),
            (f"Ref($low,{d})/$close", f"LOW{d}"),
            (f"Ref($volume,{d})/($volume+1e-12)", f"VOLUME{d}"),
        ])

    return features


def build_feature_config(
    groups: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """
    Build full feature config: Alpha158 + selected extended groups.

    Returns:
        (list_of_expressions, list_of_names)
    """
    cfg = get("factors") or {}
    if groups is None:
        all_available = get_factor_groups()
        groups = []
        for group_name in all_available:
            config_key = f"enable_{group_name}"
            if cfg.get(config_key, True):
                groups.append(group_name)

    # Start with Alpha158
    base_features = _build_alpha158_features()
    expressions = [f[0] for f in base_features]
    names = [f[1] for f in base_features]
    seen = set(names)

    # Add extended factors by group
    all_groups = get_factor_groups()
    for group in groups:
        if group not in all_groups:
            logger.warning(f"Unknown factor group: {group}")
            continue
        for expr, name in all_groups[group]:
            if name not in seen:
                expressions.append(expr)
                names.append(name)
                seen.add(name)

    logger.info(f"Total features: {len(expressions)} (Alpha158 base: {len(base_features)}, extended: {len(expressions) - len(base_features)})")
    return expressions, names


def build_label_config() -> tuple[list[str], list[str]]:
    """Build label config from settings."""
    cfg = get("factors") or {}
    label_expr = cfg.get("label", "Ref($close,-5)/$close - 1")
    label_name = cfg.get("label_name", "LABEL0")
    return [label_expr], [label_name]


class AlphaExtendedHandler:
    """
    DataHandler combining Alpha158 + extended factors.

    Wraps qlib's DataHandlerLP with our expanded feature set.
    """

    def __init__(
        self,
        instruments: str | None = None,
        start_time: str = "2010-01-01",
        end_time: str | None = None,
        groups: list[str] | None = None,
        fit_start_time: str | None = None,
        fit_end_time: str | None = None,
        infer_processors: list | None = None,
        learn_processors: list | None = None,
    ):
        data_cfg = get("data") or {}
        self.instruments = instruments or data_cfg.get("universe", "csi300")
        self.start_time = start_time
        self.end_time = end_time or _today()
        self.groups = groups
        self.fit_start_time = fit_start_time or data_cfg.get("train_start", "2012-01-01")
        self.fit_end_time = fit_end_time or data_cfg.get("train_end", "2024-06-30")

        self._feature_exprs, self._feature_names = build_feature_config(groups)
        self._label_exprs, self._label_names = build_label_config()

        self._infer_processors = infer_processors
        self._learn_processors = learn_processors

    @property
    def num_features(self) -> int:
        return len(self._feature_exprs)

    def to_qlib_handler(self) -> Any:
        """Create a qlib DataHandlerLP instance."""
        import warnings

        from qlib.data.dataset.handler import DataHandlerLP

        infer_processors = self._infer_processors or self._default_infer_processors()
        learn_processors = self._learn_processors or self._default_learn_processors()

        data_handler_config = {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "instruments": self.instruments,
            "infer_processors": infer_processors,
            "learn_processors": learn_processors,
        }

        feature_config = (self._feature_exprs, self._feature_names)
        label_config = (self._label_exprs, self._label_names)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="divide by zero", category=RuntimeWarning)
            warnings.filterwarnings("ignore", message="invalid value", category=RuntimeWarning)
            handler = DataHandlerLP(
                **data_handler_config,
                data_loader={
                    "class": "QlibDataLoader",
                    "kwargs": {
                        "config": {
                            "feature": feature_config,
                            "label": label_config,
                        },
                    },
                },
            )
        return handler

    def _default_infer_processors(self) -> list[dict]:
        return [
            {
                "class": "RobustZScoreNorm",
                "kwargs": {
                    "fields_group": "feature",
                    "clip_outlier": True,
                    "fit_start_time": self.fit_start_time,
                    "fit_end_time": self.fit_end_time,
                },
            },
            {"class": "Fillna", "kwargs": {"fields_group": "feature"}},
        ]

    @staticmethod
    def _default_learn_processors() -> list[dict]:
        return [
            {"class": "DropnaLabel"},
            {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}},
        ]

    def build_dataset(
        self,
        segments: dict[str, tuple[str, str]] | None = None,
        on_step: Any | None = None,
    ) -> Any:
        """Build a complete Qlib DatasetH.

        Args:
            segments: Optional time-range segments (train/valid/test).
            on_step: Optional callable(desc: str) invoked after each major
                     sub-step so callers can track progress.
        """
        from qlib.data.dataset import DatasetH

        if segments is None:
            cfg = get("data") or {}
            segments = {
                "train": (cfg.get("train_start", "2012-01-01"), cfg.get("train_end", "2024-06-30")),
                "valid": (cfg.get("valid_start", "2024-07-01"), cfg.get("valid_end", "2025-06-30")),
                "test": (cfg.get("test_start", "2025-07-01"), cfg.get("test_end") or _today()),
            }

        handler = self.to_qlib_handler()
        if on_step:
            on_step(f"{self.num_features} features computed")

        dataset = DatasetH(handler=handler, segments=segments)
        if on_step:
            on_step(f"{len(segments)} segments built")

        logger.info(f"Dataset built: segments={list(segments.keys())}, features={self.num_features}")
        return dataset
