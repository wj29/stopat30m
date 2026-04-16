"""System management API: data status, config, downloads, model lab."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Body, Depends
from loguru import logger

from stopat30m.auth.deps import require_role
from stopat30m.config import get, load_config, _DEFAULT_CONFIG_PATH
from stopat30m.data.provider import data_exists, get_data_dir

router = APIRouter(prefix="/system", dependencies=[Depends(require_role("admin"))])


@router.get("/data-status")
def data_status() -> dict:
    """Get data layer status: Qlib data availability, meta watermark, etc."""
    data_dir = get_data_dir()
    result = {
        "data_dir": str(data_dir),
        "data_available": data_exists(),
        "calendar_exists": (data_dir / "calendars" / "day.txt").exists(),
    }

    meta_path = data_dir / "data_meta.json"
    if meta_path.exists():
        import json
        try:
            meta = json.loads(meta_path.read_text())
            result["trusted_until"] = meta.get("trusted_until", "")
            result["last_append"] = meta.get("last_append", "")
            result["stock_count"] = len(meta.get("stocks", {}))
        except Exception:
            pass

    return result


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
    for section in _EDITABLE_SECTIONS:
        if section in cfg:
            result[section] = cfg[section]
    return result


@router.put("/model-config")
def update_model_config(payload: dict[str, Any] = Body(...)) -> dict[str, str]:
    """Merge submitted config sections back into config.yaml."""
    import stopat30m.config as config_mod

    config_path = _DEFAULT_CONFIG_PATH
    with open(config_path, "r", encoding="utf-8") as f:
        full_cfg = yaml.safe_load(f) or {}

    changed: list[str] = []
    for section in _EDITABLE_SECTIONS:
        if section in payload:
            full_cfg[section] = _deep_merge(full_cfg.get(section, {}), payload[section])
            changed.append(section)

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(full_cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    config_mod._config_cache = None
    logger.info(f"Model config updated (sections: {changed})")
    return {"status": "ok", "updated_sections": ", ".join(changed)}


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
