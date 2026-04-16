# -*- coding: utf-8 -*-
"""Agent skills package — pluggable trading skills from YAML."""

from stopat30m.agent.skills.base import Skill, SkillManager, load_skill_from_yaml, load_skills_from_directory

__all__ = ["Skill", "SkillManager", "load_skill_from_yaml", "load_skills_from_directory"]


def __getattr__(name):
    if name == "SkillAgent":
        from stopat30m.agent.skills.skill_agent import SkillAgent
        return SkillAgent
    if name == "SkillRouter":
        from stopat30m.agent.skills.router import SkillRouter
        return SkillRouter
    if name == "SkillAggregator":
        from stopat30m.agent.skills.aggregator import SkillAggregator
        return SkillAggregator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
