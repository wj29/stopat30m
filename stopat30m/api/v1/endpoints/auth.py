"""Authentication endpoints: register, login, me, change-password."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.auth.service import create_access_token, hash_password, verify_password
from stopat30m.storage.models import InviteCode, User

router = APIRouter(prefix="/auth")


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=6)
    invite_code: str = Field(..., min_length=1)


class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str = Field(..., min_length=6)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


class UserInfo(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool
    created_at: str
    last_login: str | None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/register", response_model=TokenResponse)
def register(req: RegisterRequest, db: Session = Depends(get_db_session)) -> TokenResponse:
    existing = db.query(User).filter(User.username == req.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="用户名已存在")

    invite = (
        db.query(InviteCode)
        .filter(
            InviteCode.code == req.invite_code,
            InviteCode.used_by == None,  # noqa: E711
        )
        .first()
    )
    if invite is None:
        raise HTTPException(status_code=400, detail="邀请码无效或已被使用")

    now = datetime.now(timezone.utc)
    if invite.expires_at and invite.expires_at.replace(tzinfo=timezone.utc) < now:
        raise HTTPException(status_code=400, detail="邀请码已过期")

    user = User(
        username=req.username,
        password_hash=hash_password(req.password),
        role="user",
        created_at=now,
    )
    db.add(user)
    db.flush()

    invite.used_by = user.id
    invite.used_at = now

    token = create_access_token(user.id, user.role, user.username)
    return TokenResponse(access_token=token, role=user.role, username=user.username)


@router.post("/login", response_model=TokenResponse)
def login(req: LoginRequest, db: Session = Depends(get_db_session)) -> TokenResponse:
    user = db.query(User).filter(User.username == req.username).first()
    if user is None or not verify_password(req.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或密码错误",
        )
    if not user.is_active:
        raise HTTPException(status_code=403, detail="账户已被禁用")

    user.last_login = datetime.now(timezone.utc)

    token = create_access_token(user.id, user.role, user.username)
    return TokenResponse(access_token=token, role=user.role, username=user.username)


@router.get("/me", response_model=UserInfo)
def get_me(user: User = Depends(get_current_user)) -> UserInfo:
    return UserInfo(
        id=user.id,
        username=user.username,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at.isoformat() if user.created_at else "",
        last_login=user.last_login.isoformat() if user.last_login else None,
    )


@router.post("/change-password")
def change_password(
    req: ChangePasswordRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    if not verify_password(req.old_password, user.password_hash):
        raise HTTPException(status_code=400, detail="旧密码错误")

    user.password_hash = hash_password(req.new_password)
    return {"ok": True, "message": "密码已更新"}
