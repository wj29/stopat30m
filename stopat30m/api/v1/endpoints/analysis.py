"""Analysis API endpoints."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from stopat30m.analysis.pipeline import analyze_stock, analyze_stock_streaming
from stopat30m.analysis.schemas import AnalysisRequest, FullAnalysisResponse
from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import AnalysisHistory, User

router = APIRouter(prefix="/analysis")


@router.post("/analyze", response_model=FullAnalysisResponse)
def trigger_analysis(req: AnalysisRequest, user: User = Depends(get_current_user)) -> FullAnalysisResponse:
    """Run full analysis pipeline for a stock code (blocking)."""
    result = analyze_stock(req.code, user_id=user.id)
    return result


@router.post("/analyze-stream")
def trigger_analysis_stream(req: AnalysisRequest, user: User = Depends(get_current_user)) -> StreamingResponse:
    """Run analysis with SSE progress events.

    Emits ``data: {"type":"progress", ...}`` lines, then ``data: {"type":"result", ...}``.
    """

    def _generate():
        for event in analyze_stock_streaming(req.code, user_id=user.id):
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/history")
def get_history(
    limit: int = 20,
    offset: int = 0,
    code: str | None = None,
    show_all: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    """List analysis history with total count for pagination."""
    query = db.query(AnalysisHistory).order_by(AnalysisHistory.analysis_date.desc())
    if not (show_all and user.role == "admin"):
        query = query.filter(
            (AnalysisHistory.user_id == user.id) | (AnalysisHistory.user_id == None)  # noqa: E711
        )
    if code:
        from stopat30m.data.normalize import normalize_instrument
        query = query.filter(AnalysisHistory.code == normalize_instrument(code))
    total = query.count()
    records = query.offset(offset).limit(limit).all()
    return {"total": total, "items": [_record_to_dict(r) for r in records]}


# ---------------------------------------------------------------------------
# Market review background task
# ---------------------------------------------------------------------------


class ReviewStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass
class ReviewStep:
    message: str
    done: bool = False
    duration_ms: int = 0


@dataclass
class ReviewJob:
    status: ReviewStatus = ReviewStatus.IDLE
    started_at: float = 0.0
    finished_at: float = 0.0
    steps: list[ReviewStep] = field(default_factory=list)
    report: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        elapsed = 0.0
        if self.started_at > 0:
            end = self.finished_at if self.finished_at > 0 else time.time()
            elapsed = end - self.started_at
        return {
            "status": self.status.value,
            "elapsed_sec": round(elapsed, 1),
            "steps": [
                {"message": s.message, "done": s.done, "duration_ms": s.duration_ms}
                for s in self.steps
            ],
            "report": self.report,
            "error": self.error,
        }


_review_lock = threading.Lock()
_review_job = ReviewJob()


def _run_market_review_bg() -> None:
    """Execute market review in background thread with step tracking."""
    from loguru import logger

    global _review_job
    job = _review_job
    job.status = ReviewStatus.RUNNING
    job.started_at = time.time()
    job.finished_at = 0.0
    job.report = ""
    job.error = ""
    job.steps = []

    def _step(msg: str) -> int:
        idx = len(job.steps)
        job.steps.append(ReviewStep(message=msg))
        return idx

    def _done(idx: int, t0: float) -> None:
        s = job.steps[idx]
        s.done = True
        s.duration_ms = int((time.time() - t0) * 1000)

    try:
        from stopat30m.analysis.llm_analyzer import LLMAnalyzer
        from stopat30m.analysis.market_analyzer import MarketAnalyzer
        from stopat30m.analysis.search_service import get_search_service
        from stopat30m.notification.service import NotificationService

        # Step 1: init
        i = _step("初始化分析组件")
        t0 = time.time()
        analyzer = LLMAnalyzer()
        search_svc = get_search_service()
        notifier = NotificationService()
        market = MarketAnalyzer(search_service=search_svc, analyzer=analyzer)
        _done(i, t0)

        # Step 2: indices
        i = _step("获取主要指数行情")
        t0 = time.time()
        overview = market.get_market_overview()
        _done(i, t0)

        # Step 3: news
        i = _step("搜索市场新闻")
        t0 = time.time()
        news = market.search_market_news()
        _done(i, t0)

        # Step 4: LLM review
        i = _step("大模型生成复盘报告")
        t0 = time.time()
        report = market.generate_market_review(overview, news)
        _done(i, t0)

        # Step 5: save
        i = _step("保存报告")
        t0 = time.time()
        if report:
            date_str = datetime.now().strftime("%Y%m%d")
            notifier.save_report_to_file(
                f"# 🎯 大盘复盘\n\n{report}",
                f"market_review_{date_str}.md",
            )
        _done(i, t0)

        # Step 6: notify
        i = _step("推送通知")
        t0 = time.time()
        if report and notifier.is_available():
            notifier.send(f"🎯 大盘复盘\n\n{report}", email_send_to_all=True)
        _done(i, t0)

        job.report = report or ""
        job.status = ReviewStatus.DONE
        logger.info(f"大盘复盘完成，报告长度 {len(job.report)} 字符")
    except Exception as exc:
        logger.error(f"大盘复盘失败: {exc}")
        job.error = str(exc)
        job.status = ReviewStatus.FAILED
    finally:
        job.finished_at = time.time()


@router.post("/market-review")
def trigger_market_review(user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Trigger market review as a background task. Returns immediately."""
    global _review_job
    with _review_lock:
        if _review_job.status == ReviewStatus.RUNNING:
            return {
                "triggered": False,
                "message": "复盘正在进行中，请勿重复触发",
                **_review_job.to_dict(),
            }
        _review_job = ReviewJob()
        t = threading.Thread(target=_run_market_review_bg, daemon=True)
        t.start()
    return {"triggered": True, "message": "复盘已触发，后台执行中", **_review_job.to_dict()}


@router.get("/market-review/status")
def get_market_review_status(user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Poll the current market review job status."""
    return _review_job.to_dict()


@router.get("/market-review/stream")
def stream_market_review_status(user: User = Depends(get_current_user)) -> StreamingResponse:
    """SSE stream for market review progress. Completes when job finishes."""
    import queue as _queue

    def _generate():
        last_step_count = 0
        while True:
            d = _review_job.to_dict()
            step_count = sum(1 for s in d["steps"] if s["done"])
            status = d["status"]

            if step_count > last_step_count or status in ("done", "failed", "idle"):
                yield f"data: {json.dumps(d, ensure_ascii=False)}\n\n"
                last_step_count = step_count

            if status in ("done", "failed", "idle"):
                break
            time.sleep(1)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/{analysis_id}")
def get_detail(analysis_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    """Get a single analysis record by ID."""
    record = db.query(AnalysisHistory).filter(AnalysisHistory.id == analysis_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Analysis record not found")
    return _record_to_dict(record)


@router.delete("/{analysis_id}")
def delete_analysis(analysis_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    """Delete an analysis record."""
    record = db.query(AnalysisHistory).filter(AnalysisHistory.id == analysis_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Analysis record not found")
    db.delete(record)
    return {"deleted": True, "id": analysis_id}


@router.post("/batch-delete")
def batch_delete(
    payload: dict,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    """Delete multiple analysis records by IDs."""
    ids: list[int] = payload.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="No IDs provided")
    deleted = (
        db.query(AnalysisHistory)
        .filter(AnalysisHistory.id.in_(ids))
        .delete(synchronize_session="fetch")
    )
    return {"deleted": deleted}


def _record_to_dict(r: AnalysisHistory) -> dict:
    return {
        "id": r.id,
        "code": r.code,
        "name": r.name,
        "analysis_date": r.analysis_date.isoformat() if r.analysis_date else "",
        "signal_score": r.signal_score,
        "buy_signal": r.buy_signal,
        "signal_reasons": r.signal_reasons or [],
        "risk_factors": r.risk_factors or [],
        "trend_status": r.trend_status,
        "technical_detail": r.technical_detail or {},
        "model_score": r.model_score,
        "model_percentile": r.model_percentile,
        "llm_sentiment": r.llm_sentiment,
        "llm_operation_advice": r.llm_operation_advice,
        "llm_confidence": r.llm_confidence,
        "llm_summary": r.llm_summary,
        "llm_dashboard": r.llm_dashboard or {},
        "data_source": r.data_source or "",
        "processing_time_ms": r.processing_time_ms,
    }
