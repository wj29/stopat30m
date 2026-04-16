"""Password hashing (PBKDF2-SHA256, stdlib) and JWT token management."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

from stopat30m.config import get

_ITERATIONS = 100_000


# ---------------------------------------------------------------------------
# Password hashing — zero extra dependencies
# ---------------------------------------------------------------------------


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _ITERATIONS)
    return f"{salt}${h.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt, expected = stored.split("$", 1)
    except ValueError:
        return False
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _ITERATIONS)
    return secrets.compare_digest(h.hex(), expected)


# ---------------------------------------------------------------------------
# JWT — requires python-jose[cryptography]
# ---------------------------------------------------------------------------

_secret_key: str | None = None


def _get_secret_key() -> str:
    global _secret_key
    if _secret_key is not None:
        return _secret_key

    cfg_key = str(get("auth", "secret_key", "") or "").strip()
    if cfg_key:
        _secret_key = cfg_key
        return _secret_key

    generated = secrets.token_urlsafe(48)
    _secret_key = generated
    logger.warning(
        "auth.secret_key is empty — generated ephemeral key. "
        "Set auth.secret_key in config.yaml for persistent sessions."
    )
    return _secret_key


def _get_expire_hours() -> int:
    return int(get("auth", "token_expire_hours", 24) or 24)


def create_access_token(user_id: int, role: str, username: str) -> str:
    from jose import jwt

    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "role": role,
        "username": username,
        "exp": now + timedelta(hours=_get_expire_hours()),
        "iat": now,
    }
    return jwt.encode(payload, _get_secret_key(), algorithm="HS256")


def decode_access_token(token: str) -> dict[str, Any] | None:
    from jose import JWTError, jwt

    try:
        return jwt.decode(token, _get_secret_key(), algorithms=["HS256"])
    except JWTError:
        return None
