"""System management API: data status, config, downloads, model lab."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, model_validator
from typing import Literal
from loguru import logger

from stopat30m.auth.deps import require_role
from stopat30m.config import get, load_config, _DEFAULT_CONFIG_PATH
from stopat30m.data.provider import get_data_dir
from stopat30m.data.health import inspect_data
from stopat30m.data.maintenance import UpdateBusyError, start_update, update_status

router = APIRouter(prefix="/system", dependencies=[Depends(require_role("admin"))])


@router.get("/data-status")
def data_status() -> dict:
    """Inspect coverage and dates without claiming that a local calendar is current."""
    return inspect_data(get_data_dir())


class DataUpdateRequest(BaseModel):
    source: Literal["baostock", "akshare", "efinance", "tushare"] = "baostock"
    start_date: date | None = None
    end_date: date | None = None

    @model_validator(mode="after")
    def check_dates(self):
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("开始日期不能晚于结束日期")
        from datetime import datetime
        from zoneinfo import ZoneInfo
        if self.end_date and self.end_date > datetime.now(ZoneInfo("Asia/Shanghai")).date():
            raise ValueError("结束日期不能在未来")
        return self


@router.get("/data-update")
def get_data_update() -> dict | None:
    return update_status(get_data_dir())


@router.post("/data-update", status_code=202)
def request_data_update(payload: DataUpdateRequest) -> dict:
    try:
        return start_update(
            get_data_dir(), source=payload.source,
            start_date=payload.start_date.isoformat() if payload.start_date else None,
            end_date=payload.end_date.isoformat() if payload.end_date else None,
        )
    except UpdateBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/config")
def get_config_summary() -> dict:
    """Get non-sensitive configuration summary (API keys are masked)."""
    llm_cfg = get("llm") or {}

    def _mask(key: str) -> str:
        v = str(llm_cfg.get(key, "")).strip()
        if not v:
            return ""
        return v[:4] + "****" + v[-4:] if len(v) > 12 else "****"

    return {
        "qlib_provider_uri": get("qlib", "provider_uri", ""),
        "universe": get("data", "universe", "csi300"),
        "model_type": get("model", "type", "lgbm"),
        "signal_method": get("signal", "method", "top_k"),
        "signal_top_k": get("signal", "top_k", 10),
        "llm_enabled": llm_cfg.get("enabled", False),
        "llm_model": llm_cfg.get("model", ""),
        "llm_keys_configured": {
            "deepseek": bool(llm_cfg.get("deepseek_api_key", "").strip()),
            "openai": bool(llm_cfg.get("openai_api_key", "").strip()),
            "gemini": bool(llm_cfg.get("gemini_api_key", "").strip()),
            "anthropic": bool(llm_cfg.get("anthropic_api_key", "").strip()),
            "aihubmix": bool(llm_cfg.get("aihubmix_api_key", "").strip()),
            "ollama": bool(llm_cfg.get("ollama_api_base", "").strip()),
        },
        "llm_base_urls": {
            "deepseek": llm_cfg.get("deepseek_base_url", "") or "(default)",
            "openai": llm_cfg.get("openai_base_url", "") or "(default)",
            "ollama": llm_cfg.get("ollama_api_base", "") or "(not set)",
        },
    }


@router.get("/models")
def list_models() -> list[dict]:
    """List available trained models."""
    model_dir = Path("./output/models")
    if not model_dir.exists():
        return []
    return [
        {"name": f.name, "size_mb": round(f.stat().st_size / 1024 / 1024, 2)}
        for f in sorted(model_dir.glob("*.pkl"))
    ]


# ---------------------------------------------------------------------------
# Model Lab: config read/write + CLI command generation
# ---------------------------------------------------------------------------

_EDITABLE_SECTIONS = ("model", "factors", "data", "backtest", "signal", "signal_backtest", "account_backtest")


@router.get("/model-config")
def get_model_config() -> dict[str, Any]:
    """Return the model-related config sections for the lab editor."""
    cfg = load_config()
    result: dict[str, Any] = {}
    def safe_value(value):
        if isinstance(value, dict):
            return {key: None if any(word in str(key).lower() for word in ("api_key", "password", "secret", "token"))
                    else safe_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [safe_value(item) for item in value]
        return value
    for section in _EDITABLE_SECTIONS:
        if section in cfg:
            result[section] = safe_value(cfg[section])
    return result


@router.put("/model-config")
def update_model_config(payload: Any = Body(...), user=Depends(require_role("admin"))) -> dict[str, str]:
    """Keep the legacy shape while writing only validated ignored local overrides."""
    from stopat30m.settings_store import SettingsStore, SettingsError
    try:
        return SettingsStore(config_path=_DEFAULT_CONFIG_PATH).update_legacy(payload,
            user_id=getattr(user, "id", None), username=getattr(user, "username", ""))
    except SettingsError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None
    except (OSError, ValueError, TypeError):
        raise HTTPException(status_code=409, detail="Local model settings could not be saved safely.") from None


def _deep_merge(base: Any, override: Any) -> Any:
    """Recursively merge override into base dict, replacing leaf values."""
    if not isinstance(base, dict) or not isinstance(override, dict):
        return override
    merged = dict(base)
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


@router.post("/generate-train-command")
def generate_train_command(payload: dict[str, Any] = Body(...)) -> dict[str, str]:
    """Generate a copy-pasteable CLI command from the given config."""
    model_type = payload.get("model_type", "")
    universe = payload.get("universe", "")
    save_name = payload.get("save_name", "model")
    factor_groups = payload.get("factor_groups", "")
    top_k = payload.get("top_k", 0)

    parts = ["python main.py train"]
    if model_type:
        parts.append(f"--model-type {model_type}")
    if universe:
        parts.append(f"--universe {universe}")
    if save_name and save_name != "model":
        parts.append(f"--save-name {save_name}")
    if factor_groups:
        parts.append(f"--factor-groups {factor_groups}")
    if top_k and int(top_k) > 0:
        parts.append(f"--top-k {int(top_k)}")

    return {"command": " ".join(parts)}
