# -*- coding: utf-8 -*-
"""
Trading skill base classes and SkillManager.

Skills are pluggable trading analysis modules defined in YAML.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

_BUILTIN_SKILLS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "strategies"


@dataclass
class Skill:
    """A trading skill loaded from YAML, injected into agent prompts."""
    name: str
    display_name: str
    description: str
    instructions: str
    category: str = "trend"
    core_rules: List[int] = field(default_factory=list)
    required_tools: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)
    enabled: bool = False
    source: str = "builtin"
    default_active: bool = False
    default_router: bool = False
    default_priority: int = 100
    market_regimes: List[str] = field(default_factory=list)


def load_skill_from_yaml(filepath: Union[str, Path]) -> Skill:
    """Load a single Skill from a YAML file."""
    import yaml

    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Skill file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid skill file: {filepath}")

    required = ["name", "display_name", "description", "instructions"]
    missing = [fld for fld in required if not data.get(fld)]
    if missing:
        raise ValueError(f"Skill {filepath.name} missing: {missing}")

    def _str_list(val):
        if val is None:
            return []
        if isinstance(val, str):
            return [s.strip() for s in val.split(",") if s.strip()]
        if isinstance(val, list):
            return [str(v).strip() for v in val if str(v).strip()]
        return []

    return Skill(
        name=str(data["name"]).strip(),
        display_name=str(data["display_name"]).strip(),
        description=str(data["description"]).strip(),
        instructions=str(data["instructions"]).strip(),
        category=str(data.get("category", "trend")).strip(),
        core_rules=data.get("core_rules", []) or [],
        required_tools=data.get("required_tools", []) or [],
        aliases=_str_list(data.get("aliases")),
        enabled=False,
        source=str(filepath),
        default_active=bool(data.get("default_active", False)),
        default_router=bool(data.get("default_router", False)),
        default_priority=int(data.get("default_priority", 100)),
        market_regimes=_str_list(data.get("market_regimes") or data.get("market-regimes")),
    )


def load_skills_from_directory(directory: Union[str, Path]) -> List[Skill]:
    """Load all YAML skills from a directory."""
    directory = Path(directory)
    if not directory.is_dir():
        logger.warning("Skill directory not found: %s", directory)
        return []

    skills: List[Skill] = []
    for fp in sorted(directory.glob("*.yaml")) + sorted(directory.glob("*.yml")):
        try:
            skills.append(load_skill_from_yaml(fp))
        except Exception as e:
            logger.warning("Failed to load skill %s: %s", fp.name, e)
    return skills


class SkillManager:
    """Manages trading skills and generates combined prompt instructions."""

    def __init__(self):
        self._skills: Dict[str, Skill] = {}

    def register(self, skill: Skill) -> None:
        self._skills[skill.name] = skill

    def load_builtin_skills(self) -> int:
        if not _BUILTIN_SKILLS_DIR.is_dir():
            logger.warning("Built-in skill dir not found: %s", _BUILTIN_SKILLS_DIR)
            return 0
        skills = load_skills_from_directory(_BUILTIN_SKILLS_DIR)
        for s in skills:
            s.source = "builtin"
            self.register(s)
        logger.info("Loaded %d built-in skills from %s", len(skills), _BUILTIN_SKILLS_DIR)
        return len(skills)

    def load_custom_skills(self, directory: Union[str, Path, None]) -> int:
        if not directory:
            return 0
        directory = Path(directory)
        if not directory.is_dir():
            return 0
        skills = load_skills_from_directory(directory)
        for s in skills:
            self.register(s)
        return len(skills)

    def get(self, name: str) -> Optional[Skill]:
        return self._skills.get(name)

    def list_skills(self) -> List[Skill]:
        return list(self._skills.values())

    def list_active_skills(self) -> List[Skill]:
        return [s for s in self._skills.values() if s.enabled]

    def activate(self, skill_names: List[str]) -> None:
        if "all" in skill_names:
            for s in self._skills.values():
                s.enabled = True
            return
        for s in self._skills.values():
            s.enabled = s.name in skill_names
        activated = [s.name for s in self._skills.values() if s.enabled]
        logger.info("Activated skills: %s", activated)

    def get_skill_instructions(self) -> str:
        active = self.list_active_skills()
        if not active:
            return ""

        categories = {"trend": "趋势", "pattern": "形态", "reversal": "反转", "framework": "框架"}
        grouped: Dict[str, List[Skill]] = {}
        for s in active:
            grouped.setdefault(s.category or "trend", []).append(s)

        parts = []
        idx = 1
        for cat_key in ["trend", "pattern", "reversal", "framework"] + [k for k in grouped if k not in ("trend", "pattern", "reversal", "framework")]:
            skills_in_cat = grouped.get(cat_key, [])
            if not skills_in_cat:
                continue
            cat_label = categories.get(cat_key, cat_key)
            parts.append(f"#### {cat_label}类技能\n")
            for s in skills_in_cat:
                rules_ref = ""
                if s.core_rules:
                    rules_ref = f"（关联核心理念：第{'、'.join(str(r) for r in s.core_rules)}条）"
                parts.append(
                    f"### 技能 {idx}: {s.display_name} {rules_ref}\n\n"
                    f"**适用场景**: {s.description}\n\n"
                    f"{s.instructions}\n"
                )
                idx += 1

        return "\n".join(parts)

    def get_required_tools(self) -> List[str]:
        tools: set = set()
        for s in self.list_active_skills():
            tools.update(s.required_tools)
        return list(tools)
