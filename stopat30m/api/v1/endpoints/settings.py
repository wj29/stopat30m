"""Admin-only, secret-safe persistent personal configuration."""
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from stopat30m.auth.deps import require_role
from stopat30m.settings_store import SettingsError, SettingsStore

_admin = require_role("admin")
router = APIRouter(prefix="/system", dependencies=[Depends(_admin)])


@lru_cache(maxsize=1)
def get_settings_store():
    return SettingsStore()


def _call(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except SettingsError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None
    except (OSError, ValueError, TypeError):
        # OS/YAML/validation exception excerpts may contain private values.
        raise HTTPException(status_code=409, detail="Settings could not be processed safely; no values were disclosed.") from None


@router.get("/settings")
def get_settings(store: SettingsStore = Depends(get_settings_store)):
    return _call(store.get)


@router.put("/settings")
def put_settings(payload: Any = Body(...), user=Depends(_admin), store: SettingsStore = Depends(get_settings_store)):
    if (not isinstance(payload, dict) or set(payload) - {"revision", "values", "clear_secrets"}
            or "revision" not in payload or "values" not in payload):
        raise HTTPException(status_code=400, detail="Expected revision, values and optional clear_secrets; submitted values are never echoed.")
    return _call(store.update, payload["revision"], payload["values"], payload.get("clear_secrets", []),
                 user_id=user.id, username=user.username)


@router.get("/settings/audit")
def settings_audit(store: SettingsStore = Depends(get_settings_store)):
    return _call(store.audit)
