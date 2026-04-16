# -*- coding: utf-8 -*-
"""
Agent factory — builds configured AgentExecutor or AgentOrchestrator.
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List, Optional

from stopat30m.config import get as cfg_get

logger = logging.getLogger(__name__)

_TOOL_REGISTRY = None
_SKILL_MANAGER_PROTOTYPE = None
_SKILL_MANAGER_CUSTOM_DIR: Any = object()


def get_tool_registry():
    """Return a cached ToolRegistry (built once, shared across requests)."""
    global _TOOL_REGISTRY
    if _TOOL_REGISTRY is not None:
        return _TOOL_REGISTRY

    from stopat30m.agent.tools.registry import ToolRegistry
    from stopat30m.agent.tools.data_tools import ALL_DATA_TOOLS
    from stopat30m.agent.tools.analysis_tools import ALL_ANALYSIS_TOOLS
    from stopat30m.agent.tools.search_tools import ALL_SEARCH_TOOLS

    registry = ToolRegistry()
    for tool_fn in ALL_DATA_TOOLS + ALL_ANALYSIS_TOOLS + ALL_SEARCH_TOOLS:
        registry.register(tool_fn)

    _TOOL_REGISTRY = registry
    logger.info("[AgentFactory] ToolRegistry cached (%d tools)", len(registry))
    return _TOOL_REGISTRY


def get_skill_manager(custom_dir: Optional[str] = None):
    """Return a deepcopy-clone of the cached SkillManager prototype."""
    global _SKILL_MANAGER_PROTOTYPE, _SKILL_MANAGER_CUSTOM_DIR

    if _SKILL_MANAGER_PROTOTYPE is not None and custom_dir == _SKILL_MANAGER_CUSTOM_DIR:
        return copy.deepcopy(_SKILL_MANAGER_PROTOTYPE)

    from stopat30m.agent.skills.base import SkillManager

    sm = SkillManager()
    sm.load_builtin_skills()

    if custom_dir:
        try:
            sm.load_custom_skills(custom_dir)
        except Exception as exc:
            logger.warning("Failed to load custom skills from %s: %s", custom_dir, exc)

    _SKILL_MANAGER_PROTOTYPE = sm
    _SKILL_MANAGER_CUSTOM_DIR = custom_dir
    logger.info("[AgentFactory] SkillManager cached (%d skills)", len(sm._skills))
    return copy.deepcopy(_SKILL_MANAGER_PROTOTYPE)


def build_agent_executor(skills: Optional[List[str]] = None):
    """Build a configured AgentExecutor or AgentOrchestrator.

    Reads ``agent`` section from config.yaml to determine arch and mode.
    """
    agent_cfg = cfg_get("agent") or {}
    arch = agent_cfg.get("arch", "single") if isinstance(agent_cfg, dict) else "single"

    from stopat30m.agent.llm_adapter import LLMToolAdapter

    registry = get_tool_registry()
    llm_adapter = LLMToolAdapter()

    custom_dir = agent_cfg.get("skill_dir") if isinstance(agent_cfg, dict) else None
    skill_manager = get_skill_manager(custom_dir)

    skills_to_activate = skills or []
    if not skills_to_activate and isinstance(agent_cfg, dict):
        skills_to_activate = agent_cfg.get("skills", []) or []
    if not skills_to_activate:
        from stopat30m.agent.skills.defaults import get_default_active_skill_ids
        skills_to_activate = get_default_active_skill_ids()

    skill_manager.activate(skills_to_activate)
    skill_instructions = skill_manager.get_skill_instructions()

    from stopat30m.agent.skills.defaults import (
        get_default_trading_skill_policy,
        get_default_technical_skill_policy,
    )

    explicit = bool(skills)
    default_policy = get_default_trading_skill_policy(explicit_skill_selection=explicit)
    tech_policy = get_default_technical_skill_policy(explicit_skill_selection=explicit)

    max_steps = 10
    timeout_s = 120
    if isinstance(agent_cfg, dict):
        max_steps = agent_cfg.get("max_steps", 10)
        timeout_s = agent_cfg.get("orchestrator_timeout_s", 120)

    if arch == "multi":
        from stopat30m.agent.orchestrator import AgentOrchestrator
        mode = agent_cfg.get("orchestrator_mode", "standard") if isinstance(agent_cfg, dict) else "standard"
        logger.info("[AgentFactory] Building AgentOrchestrator (mode=%s)", mode)
        return AgentOrchestrator(
            tool_registry=registry,
            llm_adapter=llm_adapter,
            skill_instructions=skill_instructions,
            technical_skill_policy=tech_policy,
            max_steps=max_steps,
            mode=mode,
            skill_manager=skill_manager,
            config=agent_cfg,
        )

    from stopat30m.agent.executor import AgentExecutor
    logger.info("[AgentFactory] Building AgentExecutor (single mode)")
    return AgentExecutor(
        tool_registry=registry,
        llm_adapter=llm_adapter,
        skill_instructions=skill_instructions,
        default_skill_policy=default_policy,
        max_steps=max_steps,
        timeout_seconds=float(timeout_s) if timeout_s else None,
    )
