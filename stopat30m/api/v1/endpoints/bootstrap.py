"""One-time, same-origin local setup for an empty personal installation."""

from __future__ import annotations

import ipaddress
import re
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth import service
from stopat30m.config import get
from stopat30m.storage.models import User

router = APIRouter(prefix="/auth")
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _origin_parts(value: str):
    parsed = urlsplit(value)
    if (parsed.scheme not in {"http", "https"} or parsed.username or parsed.password
            or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
        raise ValueError("Invalid origin")
    return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


def _local_setup_allowed(request: Request) -> tuple[bool, str | None]:
    try:
        if request.client is None or not ipaddress.ip_address(request.client.host).is_loopback:
            raise ValueError("Remote peer")
        if request.url.hostname not in _LOCAL_HOSTS:
            raise ValueError("Non-local host")
        if any(name in {"forwarded", "x-real-ip"} or name.startswith("x-forwarded-") for name in request.headers):
            raise ValueError("Proxied setup request")
        origin = request.headers.get("origin")
        if origin is not None and _origin_parts(origin) != _origin_parts(str(request.base_url)):
            raise ValueError("Cross-origin request")
        if request.headers.get("sec-fetch-site", "none") not in {"none", "same-origin"}:
            raise ValueError("Cross-site request")
    except (ValueError, TypeError):
        return False, "首次创建管理员仅允许从服务器本机的 localhost 或 127.0.0.1 页面完成。"
    return True, None


@router.get("/bootstrap")
def bootstrap_status(request: Request, db: Session = Depends(get_db_session)) -> dict:
    required = db.query(User.id).first() is None
    if not required:
        return {"required": False, "allowed": False, "reason": None}
    if not bool(get("auth", "single_user", True)):
        return {"required": True, "allowed": False, "reason": "网页首次初始化仅在个人模式开放。"}
    allowed, reason = _local_setup_allowed(request)
    return {"required": True, "allowed": allowed, "reason": reason}


@router.post("/bootstrap", status_code=201)
def bootstrap_admin(request: Request, payload: Any = Body(...), db: Session = Depends(get_db_session)) -> dict:
    allowed, reason = _local_setup_allowed(request)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason)
    if not bool(get("auth", "single_user", True)):
        raise HTTPException(status_code=403, detail="网页首次初始化仅在个人模式开放。")
    # Manual shape validation keeps credential values out of validation errors,
    # including accidentally submitted extra password fields.
    if (not isinstance(payload, dict) or set(payload) != {"username", "password"}
            or not isinstance(payload["username"], str) or not isinstance(payload["password"], str)):
        raise HTTPException(status_code=400, detail="请只提供用户名和密码两个文本字段。")
    username, password = payload["username"].strip(), payload["password"]
    if not 2 <= len(username) <= 50 or not re.fullmatch(r"[\w.@-]+", username):
        raise HTTPException(status_code=400, detail="用户名应为 2–50 个字母、数字、中文或 . @ - _ 字符。")
    if not 10 <= len(password) <= 128:
        raise HTTPException(status_code=400, detail="管理员密码应为 10–128 个字符。")
    try:
        # Serialize the empty-database check and account creation across workers.
        db.execute(text("BEGIN IMMEDIATE"))
        if db.query(User.id).first() is not None:
            raise HTTPException(status_code=409, detail="已有账户，首次初始化已关闭。请使用现有账户登录。")
        from stopat30m.settings_store import ensure_auth_secret

        try:
            ensure_auth_secret()
        except Exception:
            # Configuration parser/storage errors must not echo credentials.
            raise HTTPException(status_code=503, detail="无法安全保存登录签名配置，请检查本地配置文件的写入权限。") from None
        service._secret_key = None
        user = User(username=username, password_hash=service.hash_password(password), role="admin", is_active=True)
        db.add(user)
        db.flush()
        token = service.create_access_token(user.id, user.role, user.username)
        db.commit()
        return {"access_token": token, "token_type": "bearer", "role": user.role, "username": user.username}
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="初始化已由另一个请求完成，请刷新并登录。") from None
    except OperationalError:
        db.rollback()
        raise HTTPException(status_code=503, detail="账户初始化暂时忙碌，请稍后重试。") from None
