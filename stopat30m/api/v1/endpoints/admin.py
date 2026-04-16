"""Admin-only endpoints: invite codes, user management, record management."""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import require_role
from stopat30m.auth.service import hash_password
from stopat30m.config import get
from stopat30m.storage.models import (
    AnalysisHistory,
    ChatMessage,
    ChatSession,
    InviteCode,
    User,
)

router = APIRouter(prefix="/admin", dependencies=[Depends(require_role("admin"))])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class InviteCreateRequest(BaseModel):
    expire_days: int = Field(default=7, ge=1, le=365)


class UserUpdateRequest(BaseModel):
    role: str | None = None
    is_active: bool | None = None


class ResetPasswordRequest(BaseModel):
    new_password: str = Field(..., min_length=6)


# ---------------------------------------------------------------------------
# Invite codes
# ---------------------------------------------------------------------------


@router.post("/invite")
def create_invite(
    req: InviteCreateRequest,
    user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db_session),
) -> dict:
    expire_days = req.expire_days or int(get("auth", "invite_expire_days", 7) or 7)
    code = secrets.token_urlsafe(16)
    now = datetime.now(timezone.utc)

    invite = InviteCode(
        code=code,
        created_by=user.id,
        expires_at=now + timedelta(days=expire_days),
        created_at=now,
    )
    db.add(invite)
    db.flush()

    return {
        "code": invite.code,
        "expires_at": invite.expires_at.isoformat(),
        "expire_days": expire_days,
    }


@router.get("/invites")
def list_invites(db: Session = Depends(get_db_session)) -> list[dict]:
    invites = db.query(InviteCode).order_by(InviteCode.created_at.desc()).limit(50).all()
    return [
        {
            "id": inv.id,
            "code": inv.code,
            "created_by": inv.created_by,
            "used_by": inv.used_by,
            "expires_at": inv.expires_at.isoformat() if inv.expires_at else "",
            "used_at": inv.used_at.isoformat() if inv.used_at else None,
            "created_at": inv.created_at.isoformat() if inv.created_at else "",
        }
        for inv in invites
    ]


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------


@router.get("/users")
def list_users(db: Session = Depends(get_db_session)) -> list[dict]:
    users = db.query(User).order_by(User.created_at.desc()).all()
    return [
        {
            "id": u.id,
            "username": u.username,
            "role": u.role,
            "is_active": u.is_active,
            "created_at": u.created_at.isoformat() if u.created_at else "",
            "last_login": u.last_login.isoformat() if u.last_login else None,
        }
        for u in users
    ]


@router.put("/users/{user_id}")
def update_user(
    user_id: int,
    req: UserUpdateRequest,
    current: User = Depends(require_role("admin")),
    db: Session = Depends(get_db_session),
) -> dict:
    target = db.query(User).filter(User.id == user_id).first()
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")

    if target.id == current.id and req.is_active is False:
        raise HTTPException(status_code=400, detail="不能禁用自己")

    if target.id == current.id and req.role and req.role != "admin":
        raise HTTPException(status_code=400, detail="不能降级自己的角色")

    if req.role is not None:
        if req.role not in ("admin", "user"):
            raise HTTPException(status_code=400, detail="角色必须为 admin 或 user")
        target.role = req.role

    if req.is_active is not None:
        target.is_active = req.is_active

    return {
        "id": target.id,
        "username": target.username,
        "role": target.role,
        "is_active": target.is_active,
    }


@router.put("/users/{user_id}/reset-password")
def admin_reset_password(
    user_id: int,
    req: ResetPasswordRequest,
    current: User = Depends(require_role("admin")),
    db: Session = Depends(get_db_session),
) -> dict:
    target = db.query(User).filter(User.id == user_id).first()
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")

    target.password_hash = hash_password(req.new_password)
    return {"ok": True, "message": f"已重置用户 {target.username} 的密码"}


# ---------------------------------------------------------------------------
# Record management — chat sessions
# ---------------------------------------------------------------------------


@router.get("/chat-sessions")
def admin_list_chat_sessions(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    base = db.query(ChatSession).filter(ChatSession.is_deleted == False)  # noqa: E712
    total = base.count()
    rows = (
        base.order_by(ChatSession.updated_at.desc())
        .offset(offset).limit(limit).all()
    )
    user_ids = {s.user_id for s in rows}
    users = {u.id: u.username for u in db.query(User).filter(User.id.in_(user_ids)).all()} if user_ids else {}
    return {
        "total": total,
        "items": [
            {
                "id": s.id,
                "user_id": s.user_id,
                "username": users.get(s.user_id, ""),
                "title": s.title,
                "stock_code": s.stock_code,
                "updated_at": s.updated_at.isoformat() if s.updated_at else "",
                "created_at": s.created_at.isoformat() if s.created_at else "",
            }
            for s in rows
        ],
    }


@router.post("/chat-sessions/batch-delete")
def admin_batch_delete_chat_sessions(
    payload: dict,
    db: Session = Depends(get_db_session),
) -> dict:
    ids: list[int] = payload.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="No IDs provided")
    deleted = (
        db.query(ChatSession)
        .filter(ChatSession.id.in_(ids))
        .update({ChatSession.is_deleted: True}, synchronize_session="fetch")
    )
    return {"deleted": deleted}


# ---------------------------------------------------------------------------
# Record management — analysis history
# ---------------------------------------------------------------------------


@router.get("/analysis-history")
def admin_list_analysis_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    code: str | None = None,
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    base = db.query(AnalysisHistory)
    if code:
        from stopat30m.data.normalize import normalize_instrument
        base = base.filter(AnalysisHistory.code == normalize_instrument(code))
    total = base.count()
    rows = base.order_by(AnalysisHistory.analysis_date.desc()).offset(offset).limit(limit).all()
    user_ids = {r.user_id for r in rows if r.user_id}
    users = {u.id: u.username for u in db.query(User).filter(User.id.in_(user_ids)).all()} if user_ids else {}
    return {
        "total": total,
        "items": [
            {
                "id": r.id,
                "code": r.code,
                "name": r.name,
                "user_id": r.user_id,
                "username": users.get(r.user_id, "") if r.user_id else "",
                "analysis_date": r.analysis_date.isoformat() if r.analysis_date else "",
                "signal_score": r.signal_score,
                "llm_operation_advice": r.llm_operation_advice,
                "data_source": r.data_source or "",
            }
            for r in rows
        ],
    }


@router.post("/analysis-history/batch-delete")
def admin_batch_delete_analysis(
    payload: dict,
    db: Session = Depends(get_db_session),
) -> dict:
    ids: list[int] = payload.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="No IDs provided")
    deleted = (
        db.query(AnalysisHistory)
        .filter(AnalysisHistory.id.in_(ids))
        .delete(synchronize_session="fetch")
    )
    return {"deleted": deleted}


# ---------------------------------------------------------------------------
# Record management — market review
# ---------------------------------------------------------------------------


@router.get("/market-reviews")
def admin_list_market_reviews() -> list[dict[str, Any]]:
    """List saved market review markdown files."""
    review_dir = Path("./output/reports")
    if not review_dir.exists():
        return []
    files = sorted(review_dir.glob("market_review_*.md"), reverse=True)
    results: list[dict[str, Any]] = []
    for f in files[:50]:
        stat = f.stat()
        results.append({
            "filename": f.name,
            "size_bytes": stat.st_size,
            "created_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })
    return results


@router.post("/market-reviews/batch-delete")
def admin_batch_delete_market_reviews(payload: dict) -> dict:
    filenames: list[str] = payload.get("filenames", [])
    if not filenames:
        raise HTTPException(status_code=400, detail="No filenames provided")
    review_dir = Path("./output/reports")
    deleted = 0
    for name in filenames:
        if ".." in name or "/" in name:
            continue
        p = review_dir / name
        if p.exists() and p.suffix == ".md":
            p.unlink()
            deleted += 1
    return {"deleted": deleted}


# ---------------------------------------------------------------------------
# Record management — logs
# ---------------------------------------------------------------------------


@router.get("/logs")
def admin_get_logs(
    lines: int = Query(200, ge=10, le=2000),
) -> dict[str, Any]:
    """Read the tail of the application log file."""
    log_path_str = get("logging", "file", "./output/logs/stopat30m.log")
    log_path = Path(log_path_str).expanduser()
    if not log_path.exists():
        return {"path": str(log_path), "content": "", "size_bytes": 0, "total_lines": 0}
    all_lines: list[str] = []
    try:
        with open(log_path, "r", errors="replace") as f:
            all_lines = f.readlines()
    except Exception:
        return {"path": str(log_path), "content": "读取日志失败", "size_bytes": 0, "total_lines": 0}
    return {
        "path": str(log_path),
        "content": "".join(all_lines[-lines:]),
        "size_bytes": log_path.stat().st_size,
        "total_lines": len(all_lines),
    }


@router.post("/logs/clear")
def admin_clear_logs() -> dict:
    """Truncate the application log file."""
    log_path_str = get("logging", "file", "./output/logs/stopat30m.log")
    log_path = Path(log_path_str).expanduser()
    if log_path.exists():
        log_path.write_text("")
    return {"cleared": True, "path": str(log_path)}
