"""Configuration loader and management."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"

_config_cache: dict[str, Any] | None = None


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load base YAML, optional ignored local YAML, then environment overrides."""
    global _config_cache, _DEFAULT_CONFIG_PATH
    if _config_cache is not None and path is None:
        return _config_cache

    config_path = Path(path).expanduser().resolve() if path else _DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    cfg = _read_config(config_path)
    local_path = config_path.with_name(f"{config_path.stem}.local{config_path.suffix}")
    if local_path.is_file():
        cfg = _merge_config(cfg, _read_config(local_path))

    cfg = _apply_env_overrides(cfg)
    if path is not None:
        # The CLI's explicit configuration is also the file edited by the web
        # settings service and reloaded after its cache is invalidated.
        _DEFAULT_CONFIG_PATH = config_path
    _config_cache = cfg
    logger.info(f"Loaded config from {config_path}")
    return cfg


def _read_config(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        # YAML parser excerpts can contain credentials. Name the file only.
        raise ValueError(f"Invalid configuration YAML: {path.name}") from None
    if not isinstance(value, dict):
        raise ValueError(f"Configuration must be a mapping: {path.name}")
    return value


def _merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        merged[key] = _merge_config(merged[key], value) if isinstance(merged.get(key), dict) and isinstance(value, dict) else value
    return merged


def _apply_env_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    """Override config values via environment variables.

    Pattern: STOPAT30M_SECTION__KEY=value
    Example: STOPAT30M_TRADING__ACCOUNT=myaccount
    """
    prefix = "STOPAT30M_"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        parts = key[len(prefix):].lower().split("__")
        _set_nested(cfg, parts, value)
    return cfg


def _set_nested(d: dict, keys: list[str], value: str) -> None:
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    existing = d.get(keys[-1])
    if isinstance(existing, bool):
        d[keys[-1]] = value.lower() in ("true", "1", "yes")
    elif isinstance(existing, int):
        d[keys[-1]] = int(value)
    elif isinstance(existing, float):
        d[keys[-1]] = float(value)
    else:
        d[keys[-1]] = value


def get(section_or_path: str, key: str | None = None, default: Any = None) -> Any:
    """Get a config value by section and optional key, or by dotted path (e.g. ``notification.wechat_webhook_url``)."""
    cfg = load_config()
    if key is None and "." in section_or_path:
        cur: Any = cfg
        for part in section_or_path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur if cur is not None else default
    section_data = cfg.get(section_or_path, {})
    if key is None:
        return section_data if section_data else default
    return section_data.get(key, default)
