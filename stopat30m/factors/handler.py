"""
Combined DataHandler: Alpha158 base + extended market factors.

Produces ~400+ total features for ML model training.
"""

from __future__ import annotations

from datetime import datetime
import re
from typing import Any

import pandas as pd
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


def label_lookahead_sessions(expressions: list[str]) -> int:
    """Derive a conservative future horizon for simple Qlib Ref labels.

    Unknown/nested Ref syntax must declare a horizon explicitly rather than
    silently disabling the split purge. Positive Ref arguments only use history.
    """
    horizon = 0
    for expression in expressions:
        refs = re.findall(r"\bRef\s*\([^()]+,\s*([-+]?\d+)\s*\)", expression)
        if len(refs) != len(re.findall(r"\bRef\s*\(", expression)):
            raise ValueError("Custom label Ref syntax requires an explicit label_horizon_sessions")
        horizon = max(horizon, *(-int(value) for value in refs), 0)
    return horizon


def purge_label_segments(segments, calendar, horizon: int, *, names=("train", "valid")):
    """Shorten learning splits by real sessions so future labels stay inside.

    Test/inference dates are preserved by default: unknown recent labels must
    not suppress current predictions. Evaluation must discard unmatured labels.
    ``names`` may include test when constructing an explicit labelled experiment.
    """
    if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 0:
        raise ValueError("Label horizon must be a nonnegative integer")
    days = pd.DatetimeIndex(calendar)
    if days.empty or days.has_duplicates or days.hasnans or days.tz is not None or not days.is_monotonic_increasing:
        raise ValueError("Split purge requires a sorted, unique real exchange calendar")
    result, audit = {}, {}
    ordered = []
    for name, bounds in segments.items():
        if len(bounds) != 2:
            raise ValueError("Each dataset segment needs explicit start/end boundaries")
        start, end = (pd.Timestamp(value) for value in bounds)
        if pd.isna(start) or pd.isna(end) or start.tz is not None or end.tz is not None or start > end:
            raise ValueError("Invalid dataset segment date boundaries")
        ordered.append((start, end, name))
        effective = end
        if name in names and horizon:
            within = days[(days >= start) & (days <= end)]
            if len(within) <= horizon:
                raise ValueError(f"Segment {name} has insufficient real sessions after label purge")
            effective = within[-horizon - 1]
        result[name] = (start.strftime("%Y-%m-%d"), effective.strftime("%Y-%m-%d"))
        audit[name] = {"requested_start": start.strftime("%Y-%m-%d"), "requested_end": end.strftime("%Y-%m-%d"),
                       "effective_end": effective.strftime("%Y-%m-%d"), "purged_label_sessions": horizon if name in names else 0}
    for left, right in zip(sorted(ordered), sorted(ordered)[1:]):
        if left[1] >= right[0]:
            raise ValueError("Train/validation/test date segments must not overlap")
    return result, audit


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
        label_horizon_sessions: int | None = None,
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
        if label_horizon_sessions is None:
            self.label_horizon_sessions = label_lookahead_sessions(self._label_exprs)
        else:
            if isinstance(label_horizon_sessions, bool) or not isinstance(label_horizon_sessions, int) or label_horizon_sessions < 0:
                raise ValueError("Label horizon must be a nonnegative integer")
            try:
                required_horizon = label_lookahead_sessions(self._label_exprs)
            except ValueError:
                required_horizon = 0  # Explicit declaration covers custom syntax.
            if label_horizon_sessions < required_horizon:
                raise ValueError("Explicit label horizon cannot understate future Ref dependencies")
            self.label_horizon_sessions = label_horizon_sessions

        self._infer_processors = infer_processors
        self._learn_processors = learn_processors

    @property
    def num_features(self) -> int:
        return len(self._feature_exprs)

    def to_qlib_handler(self) -> Any:
        """Create a qlib DataHandlerLP instance."""
        from qlib.data.dataset.handler import DataHandlerLP

        infer_processors = self._default_infer_processors() if self._infer_processors is None else self._infer_processors
        learn_processors = self._default_learn_processors() if self._learn_processors is None else self._learn_processors

        data_handler_config = {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "instruments": self.instruments,
            "infer_processors": infer_processors,
            "learn_processors": learn_processors,
        }

        feature_config = (self._feature_exprs, self._feature_names)
        label_config = (self._label_exprs, self._label_names)

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

        from qlib.data import D
        calendar = D.calendar(start_time=self.start_time, end_time=self.end_time, freq="day")
        segments, split_audit = purge_label_segments(segments, calendar, self.label_horizon_sessions)
        if "train" in segments:
            self.fit_start_time, self.fit_end_time = segments["train"]

        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="divide by zero", category=RuntimeWarning)
            warnings.filterwarnings("ignore", message="invalid value", category=RuntimeWarning)

            handler = self.to_qlib_handler()
            if on_step:
                on_step(f"{self.num_features} features computed")

            dataset = DatasetH(handler=handler, segments=segments)
            dataset.stopat30m_split_audit = split_audit

        if on_step:
            on_step(f"{len(segments)} segments built")

        logger.info(f"Dataset built: segments={list(segments.keys())}, features={self.num_features}")
        return dataset
