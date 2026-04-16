"""Backtest API endpoints: history, launch, chart data."""

from __future__ import annotations

import csv
import io
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import BacktestRun, User

router = APIRouter(prefix="/backtest")

MODEL_ROOT = Path("./output/models")
PREDICTION_ROOT = Path("./output/predictions")
BACKTEST_ROOT = Path("./output/backtests")

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="backtest_")
_tasks: dict[str, dict[str, Any]] = {}
_tasks_lock = threading.Lock()

ALLOWED_CHART_FILES = {
    "returns.csv", "report.json",
    "daily_ic.csv", "daily_rank_ic.csv", "bucket_returns.csv",
    "topk_returns.csv", "turnover.csv", "horizon_stats.csv",
    "coverage.csv", "signal_history.csv",
    "nav.csv", "orders.csv", "fills.csv", "positions.csv",
    "risk_events.csv",
}


class BacktestRequest(BaseModel):
    kind: Literal["backtest", "signal", "account"]
    model_path: str | None = None
    pred_path: str | None = None
    tag: str = ""
    top_k: int | None = None
    rebalance_freq: int | None = None
    deal_price: str | None = Field(None, pattern="^(open|close)$")
    method: str | None = Field(None, pattern="^(top_k|long_short|quantile)$")
    horizons: list[int] | None = None
    group_count: int | None = None
    benchmark: str | None = None
    execution_price: str | None = Field(None, pattern="^(open|close)$")
    order_type: str | None = Field(None, pattern="^(market|limit)$")
    slippage_bps: float | None = None
    allow_partial_fill: bool | None = None
    participation_rate: float | None = None
    initial_capital: float | None = None
    cash_reserve_pct: float | None = None
    enable_risk_manager: bool = True


# ---------------------------------------------------------------------------
# Asset listing (models, prediction bundles)
# ---------------------------------------------------------------------------


@router.get("/models")
def list_backtest_models(user: User = Depends(get_current_user)) -> list[dict]:
    if not MODEL_ROOT.exists():
        return []
    return [
        {
            "name": f.name,
            "path": str(f),
            "size_mb": round(f.stat().st_size / 1024 / 1024, 2),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        }
        for f in sorted(MODEL_ROOT.glob("*.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)
    ]


@router.get("/predictions")
def list_predictions(user: User = Depends(get_current_user)) -> list[dict]:
    if not PREDICTION_ROOT.exists():
        return []
    return [
        {
            "name": f.name,
            "path": str(f),
            "size_mb": round(f.stat().st_size / 1024 / 1024, 2),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        }
        for f in sorted(PREDICTION_ROOT.glob("*.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)
    ]


# ---------------------------------------------------------------------------
# Run history (DB-backed)
# ---------------------------------------------------------------------------


@router.get("/runs")
def list_runs(
    kind: str | None = None,
    limit: int = 20,
    show_all: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[dict]:
    query = db.query(BacktestRun).order_by(BacktestRun.created_at.desc())
    if not (show_all and user.role == "admin"):
        query = query.filter(
            (BacktestRun.user_id == user.id) | (BacktestRun.user_id == None)  # noqa: E711
        )
    if kind:
        query = query.filter(BacktestRun.kind == kind)
    records = query.limit(limit).all()
    results = []
    for r in records:
        row: dict = {
            "id": r.id,
            "kind": r.kind,
            "tag": r.tag,
            "created_at": r.created_at.isoformat() if r.created_at else "",
            "annual_return": r.annual_return,
            "sharpe": r.sharpe,
            "max_drawdown": r.max_drawdown,
            "total_trades": r.total_trades,
            "win_rate": r.win_rate,
        }
        report = r.report or {}
        row["ic_mean"] = report.get("IC_mean") or report.get("ic_5d_mean")
        row["rank_ic_mean"] = report.get("RankIC_mean") or report.get("rank_ic_5d_mean")
        row["icir"] = report.get("ICIR")
        row["ending_equity"] = report.get("ending_equity")

        cfg = r.config or {}
        row["model_type"] = cfg.get("model_type", "")
        row["universe"] = cfg.get("universe", "")
        results.append(row)
    return results


@router.get("/runs/{run_id}")
def get_run_detail(
    run_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    record = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Backtest run not found")

    return {
        "id": record.id,
        "kind": record.kind,
        "tag": record.tag,
        "run_dir": record.run_dir,
        "created_at": record.created_at.isoformat() if record.created_at else "",
        "report": record.report or {},
        "config": record.config or {},
    }


# ---------------------------------------------------------------------------
# Chart data (read CSVs from run_dir)
# ---------------------------------------------------------------------------


@router.get("/runs/{run_id}/charts")
def get_run_charts(
    run_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    record = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")

    run_dir = Path(record.run_dir) if record.run_dir else None
    if not run_dir or not run_dir.exists():
        return {"files": {}}

    result: dict[str, list[dict]] = {}
    for f in run_dir.iterdir():
        if f.name not in ALLOWED_CHART_FILES or not f.is_file():
            continue
        if f.suffix == ".csv":
            result[f.stem] = _read_csv_as_dicts(f)
        elif f.suffix == ".json":
            try:
                result[f.stem] = json.loads(f.read_text())
            except Exception:
                pass

    return {"files": result}


def _read_csv_as_dicts(path: Path, max_rows: int = 2000) -> list[dict]:
    try:
        text = path.read_text()
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            parsed: dict[str, Any] = {}
            for k, v in row.items():
                parsed[k] = _try_parse_number(v)
            rows.append(parsed)
        return rows
    except Exception:
        return []


def _try_parse_number(v: str | None) -> Any:
    if v is None or v == "":
        return None
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v


# ---------------------------------------------------------------------------
# Filesystem compat (for CLI-generated runs not in DB)
# ---------------------------------------------------------------------------


@router.get("/dirs/{kind}")
def list_run_dirs(kind: str, user: User = Depends(get_current_user)) -> list[dict]:
    kind_dir = BACKTEST_ROOT / kind
    if not kind_dir.exists():
        return []

    runs = []
    for run_dir in sorted(kind_dir.iterdir(), reverse=True):
        if not run_dir.is_dir():
            continue
        report_path = run_dir / "report.json"
        report = {}
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text())
            except Exception:
                pass
        runs.append({
            "name": run_dir.name,
            "path": str(run_dir),
            "annual_return": report.get("annual_return"),
            "sharpe": report.get("sharpe"),
            "max_drawdown": report.get("max_drawdown"),
        })

    return runs


# ---------------------------------------------------------------------------
# Launch backtest (background execution)
# ---------------------------------------------------------------------------


@router.get("/active-task")
def get_active_task(user: User = Depends(get_current_user)) -> dict:
    """Return the currently running/pending task, or null."""
    with _tasks_lock:
        for t in _tasks.values():
            if t["status"] in ("pending", "running"):
                return dict(t)
    return {"task_id": None, "status": "idle"}


@router.post("/run")
def submit_backtest(
    body: BacktestRequest,
    user: User = Depends(get_current_user),
) -> dict:
    if not body.model_path and not body.pred_path:
        raise HTTPException(status_code=400, detail="Provide model_path or pred_path")
    if body.model_path and body.pred_path:
        raise HTTPException(status_code=400, detail="Provide only one of model_path or pred_path")

    with _tasks_lock:
        for t in _tasks.values():
            if t["status"] in ("pending", "running"):
                raise HTTPException(
                    status_code=409,
                    detail=f"A backtest is already running (task_id={t['task_id']})",
                )

    task_id = uuid.uuid4().hex
    task_state = {
        "task_id": task_id,
        "status": "running",
        "kind": body.kind,
        "progress": "",
        "run_id": None,
        "error": None,
        "created_at": datetime.utcnow().isoformat(),
    }
    with _tasks_lock:
        _tasks[task_id] = task_state

    _executor.submit(_run_backtest_task, task_id, body.model_dump(), user.id)
    return {"task_id": task_id, "status": "running"}


@router.get("/task/{task_id}")
def get_backtest_task(task_id: str, user: User = Depends(get_current_user)) -> dict:
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return dict(task)


def _run_backtest_task(task_id: str, params: dict, user_id: int) -> None:
    """Execute backtest in background thread and persist results to DB."""

    def _update(status: str, progress: str = "", **extra: Any) -> None:
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["status"] = status
                _tasks[task_id]["progress"] = progress
                _tasks[task_id].update(extra)

    try:
        _update("running", "Initializing data provider...")
        from stopat30m.data.provider import init_qlib
        init_qlib()

        _update("running", "Loading predictions...")
        kind = params["kind"]

        from stopat30m.backtest.common import load_prediction_source
        pred, labels, metadata = load_prediction_source(
            model_path=params.get("model_path"),
            pred_path=params.get("pred_path"),
        )

        if kind == "backtest":
            run_id = _run_basic_backtest(task_id, pred, params, metadata, user_id, _update)
        elif kind == "signal":
            run_id = _run_signal_backtest(task_id, pred, params, metadata, user_id, _update)
        elif kind == "account":
            run_id = _run_account_backtest(task_id, pred, params, metadata, user_id, _update)
        else:
            raise ValueError(f"Unknown kind: {kind}")

        _update("completed", "Done", run_id=run_id)

    except Exception as e:
        _update("failed", error=str(e)[:500])


def _run_basic_backtest(
    task_id: str, pred: Any, params: dict, metadata: dict,
    user_id: int, update: Any,
) -> int:
    update("running", "Running backtest engine...")
    from stopat30m.backtest.engine import BacktestEngine

    kwargs: dict = {}
    if params.get("top_k"):
        kwargs["top_k"] = params["top_k"]
    if params.get("rebalance_freq"):
        kwargs["rebalance_freq"] = params["rebalance_freq"]
    if params.get("deal_price"):
        kwargs["deal_price"] = params["deal_price"]

    engine = BacktestEngine(**kwargs)
    result = engine.run(pred)
    out_dir = result.save()

    update("running", "Saving to database...")
    return _persist_run(
        kind="backtest",
        tag=params.get("tag", ""),
        run_dir=str(out_dir),
        report=result.metrics,
        config=result.config,
        user_id=user_id,
    )


def _run_signal_backtest(
    task_id: str, pred: Any, params: dict, metadata: dict,
    user_id: int, update: Any,
) -> int:
    update("running", "Running signal backtest engine...")
    from stopat30m.backtest.signal_backtest import SignalBacktestEngine
    from stopat30m.config import get

    cfg = get("signal_backtest") or {}
    engine = SignalBacktestEngine(
        top_k=params.get("top_k") or cfg.get("top_k", 10),
        method=params.get("method") or cfg.get("method", "top_k"),
        rebalance_freq=params.get("rebalance_freq") or cfg.get("rebalance_freq", 5),
        horizons=params.get("horizons") or cfg.get("horizons", [1, 3, 5, 10, 20]),
        group_count=params.get("group_count") or cfg.get("group_count", 10),
        benchmark=params.get("benchmark") or cfg.get("benchmark", "SH000300"),
    )
    result = engine.run(pred)
    tag = params.get("tag", "")
    run_dir = result.save(tag=tag)

    update("running", "Saving to database...")
    report = {**result.summary_metrics, **result.topk_metrics}
    return _persist_run(
        kind="signal",
        tag=tag,
        run_dir=str(run_dir),
        report=report,
        config=result.config,
        user_id=user_id,
    )


def _run_account_backtest(
    task_id: str, pred: Any, params: dict, metadata: dict,
    user_id: int, update: Any,
) -> int:
    update("running", "Running account backtest engine...")
    from stopat30m.backtest.account_backtest import AccountBacktestEngine
    from stopat30m.config import get

    cfg = get("account_backtest") or {}
    engine = AccountBacktestEngine(
        initial_capital=params.get("initial_capital") or cfg.get("initial_capital", 1_000_000),
        top_k=params.get("top_k") or cfg.get("top_k", 10),
        method=params.get("method") or cfg.get("method", "top_k"),
        rebalance_freq=params.get("rebalance_freq") or cfg.get("rebalance_freq", 5),
        execution_price=params.get("execution_price") or cfg.get("execution_price", "open"),
        order_type=params.get("order_type") or cfg.get("order_type", "market"),
        slippage_bps=params["slippage_bps"] if params.get("slippage_bps") is not None else cfg.get("slippage_bps", 0.0),
        allow_partial_fill=params["allow_partial_fill"] if params.get("allow_partial_fill") is not None else cfg.get("allow_partial_fill", False),
        participation_rate=params["participation_rate"] if params.get("participation_rate") is not None else cfg.get("participation_rate", 0.1),
        cash_reserve_pct=params["cash_reserve_pct"] if params.get("cash_reserve_pct") is not None else cfg.get("cash_reserve_pct", 0.02),
        benchmark=params.get("benchmark") or cfg.get("benchmark", "SH000300"),
        enable_risk_manager=params.get("enable_risk_manager", True) and cfg.get("enable_risk_manager", True),
    )
    result = engine.run(pred)
    tag = params.get("tag", "")
    run_dir = result.save(tag=tag)

    update("running", "Saving to database...")
    return _persist_run(
        kind="account",
        tag=tag,
        run_dir=str(run_dir),
        report=result.report,
        config=result.config,
        user_id=user_id,
    )


def _persist_run(
    kind: str, tag: str, run_dir: str,
    report: dict, config: dict, user_id: int,
) -> int:
    from stopat30m.storage.database import get_db

    with get_db() as db:
        run = BacktestRun(
            kind=kind,
            user_id=user_id,
            tag=tag,
            run_dir=run_dir,
            annual_return=report.get("annual_return"),
            sharpe=report.get("sharpe"),
            max_drawdown=report.get("max_drawdown"),
            total_trades=report.get("total_trades"),
            win_rate=report.get("win_rate"),
            config=config,
            report=report,
        )
        db.add(run)
        db.flush()
        return run.id
