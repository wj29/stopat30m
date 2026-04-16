# -*- coding: utf-8 -*-
"""SkillAgent — runtime specialist adapter for a selected skill."""

from __future__ import annotations

import json
import logging
from typing import Optional

from stopat30m.agent.agents.base_agent import BaseAgent
from stopat30m.agent.protocols import AgentContext, AgentOpinion
from stopat30m.agent.runner import try_parse_json
from stopat30m.agent.skills.defaults import build_skill_agent_name

logger = logging.getLogger(__name__)


class SkillAgent(BaseAgent):
    max_steps = 4

    def __init__(self, skill_id: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if not skill_id:
            raise ValueError("skill_id is required")
        self.skill_id = skill_id
        self.agent_name = build_skill_agent_name(skill_id)
        self._skill = self._load_skill(skill_id)

        if self._skill and self._skill.required_tools:
            self.tool_names = list(self._skill.required_tools)

    @staticmethod
    def _load_skill(skill_id: str):
        try:
            from stopat30m.agent.factory import get_skill_manager
            sm = get_skill_manager()
            return sm.get(skill_id)
        except Exception as exc:
            logger.warning("[SkillAgent] failed to load '%s': %s", skill_id, exc)
        return None

    def system_prompt(self, ctx: AgentContext) -> str:
        if self._skill:
            instructions = self._skill.instructions or self._skill.description
            display = self._skill.display_name
        else:
            instructions = f"Evaluate the '{self.skill_id}' skill."
            display = self.skill_id

        return f"""\
You are a **Skill Evaluation Agent** applying the **{display}** skill.

## Skill Instructions
{instructions}

## Task
Evaluate whether current stock conditions satisfy this skill's entry criteria.
Use tools if needed.

## Output Format
Return **only** a JSON object:
{{
  "skill_id": "{self.skill_id}",
  "signal": "strong_buy|buy|hold|sell|strong_sell",
  "confidence": 0.0-1.0,
  "conditions_met": ["list of satisfied conditions"],
  "conditions_missed": ["list of unsatisfied conditions"],
  "score_adjustment": -20 to +20,
  "reasoning": "2-3 sentence skill evaluation"
}}
"""

    def build_user_message(self, ctx: AgentContext) -> str:
        parts = [
            f"Evaluate **{self.skill_id}** skill for stock "
            f"**{ctx.stock_code}** ({ctx.stock_name or 'unknown'}).",
        ]
        for op in ctx.opinions:
            if op.agent_name == "technical":
                parts.append(f"\nTechnical summary: {op.reasoning}")
                if op.key_levels:
                    parts.append(f"Key levels: {json.dumps(op.key_levels)}")
                if op.raw_data:
                    parts.append(f"Technical data: {json.dumps(op.raw_data, ensure_ascii=False, default=str)[:2000]}")
        return "\n".join(parts)

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        parsed = try_parse_json(raw_text)
        if parsed is None:
            logger.warning("[SkillAgent:%s] failed to parse JSON", self.skill_id)
            return None

        return AgentOpinion(
            agent_name=self.agent_name,
            signal=parsed.get("signal", "hold"),
            confidence=float(parsed.get("confidence", 0.5)),
            reasoning=parsed.get("reasoning", ""),
            raw_data=parsed,
        )
