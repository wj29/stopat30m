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

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import BacktestRun, User
from stopat30m.config import PROJECT_ROOT, get
from stopat30m.backtest.common import BACKTEST_LIMITATIONS
from stopat30m.settings_store import SettingsError, validate_legacy_prediction_scope, validate_legacy_universe

router = APIRouter(prefix="/backtest")

MODEL_ROOT = PROJECT_ROOT / "output" / "models"
PREDICTION_ROOT = PROJECT_ROOT / "output" / "predictions"
BACKTEST_ROOT = PROJECT_ROOT / "output" / "backtests"

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
    universe: str | None = None
    tag: str = Field("", max_length=80, pattern=r"^[\w. -]*$")
    top_k: int | None = Field(None, ge=1, le=1000)
    rebalance_freq: int | None = Field(None, ge=1, le=252)
    deal_price: str | None = Field(None, pattern="^(open|close)$")
    method: Literal["top_k"] | None = None
    horizons: list[int] | None = Field(None, min_length=1, max_length=20)
    group_count: int | None = Field(None, ge=2, le=100)
    benchmark: str | None = Field(None, min_length=1, max_length=30, pattern=r"^[A-Za-z0-9._-]+$")
    execution_price: str | None = Field(None, pattern="^(open|close)$")
    order_type: str | None = Field(None, pattern="^(market|limit)$")
    slippage_bps: float | None = Field(None, ge=0, le=1000, allow_inf_nan=False)
    allow_partial_fill: bool | None = None
    participation_rate: float | None = Field(None, gt=0, le=1, allow_inf_nan=False)
    initial_capital: float | None = Field(None, gt=0, le=1e12, allow_inf_nan=False)
    cash_reserve_pct: float | None = Field(None, ge=0, lt=1, allow_inf_nan=False)
    enable_risk_manager: bool = True


@router.get("/defaults")
def get_backtest_defaults(user: User = Depends(get_current_user)) -> dict:
    basic = get("backtest") or {}
    signal = get("signal_backtest") or {}
    account = get("account_backtest") or {}
    return {
        "backtest": {"top_k": basic.get("top_k", 20), "rebalance_freq": basic.get("rebalance_freq", 5),
                     "deal_price": basic.get("deal_price", "open"),
                     "benchmark": basic.get("benchmark") or get("data", "benchmark", "SH000300")},
        "signal": {"top_k": signal.get("top_k", 10), "rebalance_freq": signal.get("rebalance_freq", 5),
                   "method": "top_k", "horizons": signal.get("horizons", [1, 3, 5, 10, 20]),
                   "group_count": signal.get("group_count", 10), "benchmark": signal.get("benchmark", "SH000300")},
        "account": {key: account.get(key, default) for key, default in {
            "initial_capital": 1_000_000, "top_k": 10, "method": "top_k", "rebalance_freq": 5,
            "execution_price": "open", "order_type": "market", "slippage_bps": 0.0,
            "allow_partial_fill": False, "participation_rate": 0.1, "cash_reserve_pct": 0.02,
            "benchmark": "SH000300", "enable_risk_manager": True,
        }.items()} | {"method": "top_k"},
        "supported_methods": ["top_k"], "limitations": BACKTEST_LIMITATIONS,
    }


def _owned_run(db: Session, run_id: int, user: User) -> BacktestRun:
    query = db.query(BacktestRun).filter(BacktestRun.id == run_id)
    if user.role != "admin":
        query = query.filter(BacktestRun.user_id == user.id)
    record = query.first()
    if record is None:
        raise HTTPException(status_code=404, detail="Backtest run not found")
    return record


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
    limit: int = Query(20, ge=1, le=200),
    show_all: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[dict]:
    query = db.query(BacktestRun).order_by(BacktestRun.created_at.desc())
    if not (show_all and user.role == "admin"):
        query = query.filter(BacktestRun.user_id == user.id)
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
        row["ic_mean"] = report.get("IC_mean", report.get("ic_5d_mean"))
        row["rank_ic_mean"] = report.get("RankIC_mean", report.get("rank_ic_5d_mean"))
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
    record = _owned_run(db, run_id, user)

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
    record = _owned_run(db, run_id, user)

    run_dir = Path(record.run_dir) if record.run_dir else None
    if not run_dir or not run_dir.exists():
        return {"files": {}}
    if not run_dir.resolve().is_relative_to((PROJECT_ROOT / "output").resolve()):
        raise HTTPException(status_code=400, detail="Invalid backtest result directory")

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
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="CLI run directories are available to administrators only")
    if kind not in {"backtest", "signal", "account"}:
        raise HTTPException(status_code=400, detail="Unknown backtest kind")
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
            if t.get("user_id") == user.id and t["status"] in ("pending", "running"):
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
    if body.horizons and any(h < 1 or h > 252 for h in body.horizons):
        raise HTTPException(status_code=422, detail="Horizons must be between 1 and 252 trading days")
    try:
        body.universe = validate_legacy_universe(
            body.universe if "universe" in body.model_fields_set else get("data", "universe", "csi300"),
            require_audited_samples=True,
        )
    except SettingsError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None
    # These files contain pickle objects. Only server-managed model directories
    # are accepted; an HTTP client must never choose an arbitrary local pickle.
    for key, root in (("model_path", MODEL_ROOT), ("pred_path", PREDICTION_ROOT)):
        value = getattr(body, key)
        if value:
            target = Path(value)
            target = target if target.is_absolute() else PROJECT_ROOT / target
            target = target.resolve()
            if not target.is_relative_to(root.resolve()) or target.suffix != ".pkl" or not target.is_file():
                raise HTTPException(status_code=422, detail=f"Invalid {key}: select a listed server asset")
            setattr(body, key, str(target))

    if body.pred_path:
        # Inspect only a server-managed asset, before scheduling any data/model work.
        from stopat30m.backtest.common import load_prediction_bundle
        try:
            _, _, metadata = load_prediction_bundle(body.pred_path)
        except Exception:
            raise HTTPException(status_code=422, detail="无法读取预测缓存，不能验证股票池来源。") from None
        try:
            validate_legacy_prediction_scope(metadata, body.universe)
        except SettingsError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None

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
            "user_id": user.id,
        }
        _tasks[task_id] = task_state

    _executor.submit(_run_backtest_task, task_id, body.model_dump(), user.id)
    return {"task_id": task_id, "status": "running"}


@router.get("/task/{task_id}")
def get_backtest_task(task_id: str, user: User = Depends(get_current_user)) -> dict:
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task or task.get("user_id") != user.id:
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
        # Keep the accepted scope fixed even if settings change while queued.
        universe = validate_legacy_universe(params.get("universe"), require_audited_samples=True)
        _update("running", "Initializing data provider...")
        from stopat30m.data.provider import init_qlib
        init_qlib()

        _update("running", "Loading predictions...")
        kind = params["kind"]

        from stopat30m.backtest.common import load_prediction_source
        pred, labels, metadata = load_prediction_source(
            model_path=params.get("model_path"),
            pred_path=params.get("pred_path"),
            universe=universe,
        )
        # Recheck after loading: a server asset may have changed since submission.
        validate_legacy_prediction_scope(metadata, universe)

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
    if params.get("benchmark"):
        kwargs["benchmark"] = params["benchmark"]

    engine = BacktestEngine(**kwargs)
    result = engine.run(pred)
    result.config["source_metadata"] = metadata
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
    result.config["source_metadata"] = metadata
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
    result.config["source_metadata"] = metadata
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
