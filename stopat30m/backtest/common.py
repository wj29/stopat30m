from __future__ import annotations

import json
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from stopat30m.config import get
from stopat30m.data.provider import init_qlib
from stopat30m.factors.handler import AlphaExtendedHandler

BACKTEST_ROOT = Path("./output/backtests")
PREDICTION_ROOT = Path("./output/predictions")


def now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def create_run_dir(kind: str, tag: str = "") -> Path:
    suffix = f"_{tag}" if tag else ""
    run_dir = BACKTEST_ROOT / kind / f"{now_tag()}{suffix}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def list_run_dirs(kind: str) -> list[Path]:
    root = BACKTEST_ROOT / kind
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()], reverse=True)


def latest_run_dir(kind: str) -> Path | None:
    runs = list_run_dirs(kind)
    return runs[0] if runs else None


def create_prediction_path(tag: str = "") -> Path:
    suffix = f"_{tag}" if tag else ""
    PREDICTION_ROOT.mkdir(parents=True, exist_ok=True)
    return PREDICTION_ROOT / f"predictions_{now_tag()}{suffix}.pkl"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def normalize_prediction_series(predictions: pd.Series | pd.DataFrame) -> pd.Series:
    if isinstance(predictions, pd.DataFrame):
        predictions = predictions.iloc[:, 0]
    if not isinstance(predictions.index, pd.MultiIndex):
        raise ValueError("Predictions must use MultiIndex(datetime, instrument).")
    return predictions.dropna().sort_index()


def save_prediction_bundle(
    predictions: pd.Series | pd.DataFrame,
    labels: pd.Series | None = None,
    metadata: dict[str, Any] | None = None,
    path: str | Path | None = None,
    tag: str = "",
) -> Path:
    pred = normalize_prediction_series(predictions)
    bundle = {
        "predictions": pred,
        "labels": labels.sort_index() if isinstance(labels, pd.Series) else labels,
        "metadata": metadata or {},
    }
    target = Path(path) if path is not None else create_prediction_path(tag=tag)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "wb") as f:
        pickle.dump(bundle, f)
    return target


def load_prediction_bundle(path: str | Path) -> tuple[pd.Series, pd.Series | None, dict[str, Any]]:
    target = Path(path)
    with open(target, "rb") as f:
        bundle = pickle.load(f)

    if isinstance(bundle, pd.Series):
        return normalize_prediction_series(bundle), None, {}

    predictions = normalize_prediction_series(bundle["predictions"])
    labels = bundle.get("labels")
    if isinstance(labels, pd.DataFrame):
        labels = labels.iloc[:, 0]
    if isinstance(labels, pd.Series):
        labels = labels.sort_index()
    metadata = dict(bundle.get("metadata", {}))
    metadata.setdefault("source_path", str(target))
    return predictions, labels, metadata


def load_model_predictions(
    model_path: str,
    universe: str | None = None,
    factor_groups: str | None = None,
    test_only: bool = True,
) -> tuple[pd.Series, pd.Series | None, AlphaExtendedHandler]:
    init_qlib()

    with open(model_path, "rb") as f:
        model = pickle.load(f)

    groups = factor_groups.split(",") if factor_groups else None
    handler = AlphaExtendedHandler(groups=groups, instruments=universe)
    segments = _test_only_segments() if test_only else None
    dataset = handler.build_dataset(segments=segments)

    predictions = normalize_prediction_series(model.predict(dataset))

    labels = None
    try:
        raw_label = dataset.prepare("test", col_set="label", data_key="infer")
        if isinstance(raw_label, pd.DataFrame):
            labels = raw_label.iloc[:, 0]
        else:
            labels = raw_label
        if labels is not None and isinstance(labels.index, pd.MultiIndex):
            labels = labels.sort_index()
    except Exception:
        labels = None

    return predictions, labels, handler


def _test_only_segments() -> dict[str, tuple[str, str]]:
    cfg = get("data") or {}
    return {
        "test": (
            cfg.get("test_start", "2025-07-01"),
            cfg.get("test_end") or datetime.now().strftime("%Y-%m-%d"),
        ),
    }


def load_prediction_source(
    model_path: str | None = None,
    pred_path: str | None = None,
    universe: str | None = None,
    factor_groups: str | None = None,
) -> tuple[pd.Series, pd.Series | None, dict[str, Any]]:
    if bool(model_path) == bool(pred_path):
        raise ValueError("Provide exactly one of model_path or pred_path.")

    if pred_path:
        predictions, labels, metadata = load_prediction_bundle(pred_path)
        metadata.setdefault("source", "prediction_bundle")
        return predictions, labels, metadata

    predictions, labels, handler = load_model_predictions(
        model_path=model_path or "",
        universe=universe,
        factor_groups=factor_groups,
    )
    metadata = {
        "source": "model",
        "model_path": model_path,
        "universe": handler.instruments,
        "factor_groups": factor_groups.split(",") if factor_groups else None,
        "feature_count": handler.num_features,
    }
    return predictions, labels, metadata


def infer_dates_and_instruments(predictions: pd.Series) -> tuple[list[pd.Timestamp], list[str]]:
    pred = normalize_prediction_series(predictions)
    dates = sorted(pd.to_datetime(pred.index.get_level_values(0).unique()))
    instruments = sorted(str(v) for v in pred.index.get_level_values(1).unique())
    return dates, instruments


def fetch_price_fields(
    instruments: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    fields: list[str],
) -> dict[str, pd.DataFrame]:
    init_qlib()
    import qlib.data

    result: dict[str, pd.DataFrame] = {}
    for field in fields:
        df = qlib.data.D.features(
            instruments=instruments,
            fields=[field],
            start_time=pd.Timestamp(start_date),
            end_time=pd.Timestamp(end_date),
        )
        df = df.reset_index()
        value_col = field if field in df.columns else df.columns[-1]
        if "datetime" not in df.columns or "instrument" not in df.columns:
            raise ValueError(f"Unexpected qlib price frame columns: {list(df.columns)}")
        result[field] = df.pivot(index="datetime", columns="instrument", values=value_col).sort_index()
    return result


def fetch_benchmark_close(
    benchmark: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.Series:
    init_qlib()
    import qlib.data

    try:
        df = qlib.data.D.features(
            instruments=[benchmark],
            fields=["$close"],
            start_time=pd.Timestamp(start_date),
            end_time=pd.Timestamp(end_date),
        )
        return df.droplevel("instrument")["$close"].sort_index()
    except Exception:
        return pd.Series(dtype=float)


def series_to_frame(name: str, series: pd.Series) -> pd.DataFrame:
    frame = series.to_frame(name)
    frame.index.name = "date"
    return frame
