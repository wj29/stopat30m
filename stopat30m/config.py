"""Configuration loader and management."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"

_config_cache: dict[str, Any] | None = None


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load YAML config, with env-var override support."""
    global _config_cache
    if _config_cache is not None and path is None:
        return _config_cache

    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    cfg = _apply_env_overrides(cfg)
    _config_cache = cfg
    logger.info(f"Loaded config from {config_path}")
    return cfg


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


def get(section: str, key: str | None = None, default: Any = None) -> Any:
    """Get a config value by section and optional key."""
    cfg = load_config()
    section_data = cfg.get(section, {})
    if key is None:
        return section_data if section_data else default
    return section_data.get(key, default)
