"""Signal API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import SignalHistory, User

router = APIRouter(prefix="/signals")


@router.get("/latest")
def get_latest_signals(limit: int = 20, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> list[dict]:
    """Get most recent signal batch."""
    records = (
        db.query(SignalHistory)
        .order_by(SignalHistory.signal_date.desc(), SignalHistory.score.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": r.id,
            "signal_date": r.signal_date,
            "instrument": r.instrument,
            "score": r.score,
            "signal": r.signal,
            "weight": r.weight,
            "method": r.method,
        }
        for r in records
    ]


@router.get("/history")
def get_signal_history(
    limit: int = 100,
    offset: int = 0,
    instrument: str | None = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[dict]:
    """List historical signals."""
    query = db.query(SignalHistory).order_by(SignalHistory.signal_date.desc())
    if instrument:
        from stopat30m.data.normalize import normalize_instrument
        query = query.filter(SignalHistory.instrument == normalize_instrument(instrument))
    records = query.offset(offset).limit(limit).all()
    return [
        {
            "id": r.id,
            "signal_date": r.signal_date,
            "instrument": r.instrument,
            "score": r.score,
            "signal": r.signal,
            "weight": r.weight,
            "method": r.method,
            "batch_id": r.batch_id,
        }
        for r in records
    ]
