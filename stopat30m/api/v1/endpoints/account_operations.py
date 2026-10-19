"""Authenticated observed NAV and read-only standard-statement reconciliation."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import User
from stopat30m.trading import account_operations as operations
from stopat30m.trading.portfolio import PortfolioError

router = APIRouter(prefix="/trading")


class NavRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    idempotency_key: str = Field(min_length=1, max_length=100)


class ReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    csv_text: str = Field(min_length=1, max_length=100_000)
    broker_cash: float = Field(ge=0, le=1e12, strict=True)
    statement_date: str | None = None


def _call(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except PortfolioError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/nav")
def nav_history(limit: int = Query(90, ge=1, le=366), offset: int = Query(0, ge=0),
                user: User = Depends(get_current_user), db: Session = Depends(get_db_session)):
    return _call(operations.list_nav, db, user.id, limit=limit, offset=offset)


@router.post("/nav")
def save_nav(request: NavRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)):
    snapshot, replayed = _call(operations.save_nav, db, user.id, request.idempotency_key)
    db.commit()  # No successful receipt before durable transaction completion.
    return {"snapshot": snapshot, "replayed": replayed}


@router.post("/reconcile")
def reconcile(request: ReconcileRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)):
    return _call(operations.reconcile, db, user.id, **request.model_dump())
