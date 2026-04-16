# -*- coding: utf-8 -*-
"""SkillRouter — rule-based skill selection based on market regime."""

from __future__ import annotations

import logging
from typing import List, Optional

from stopat30m.agent.protocols import AgentContext
from stopat30m.agent.skills.defaults import get_default_active_skill_ids

logger = logging.getLogger(__name__)


class SkillRouter:
    def select_skills(self, ctx: AgentContext, max_count: int = 3) -> List[str]:
        requested = ctx.meta.get("skills_requested", [])
        if requested:
            logger.info("[SkillRouter] user-requested: %s", requested)
            return requested[:max_count]

        regime = self._detect_regime(ctx)
        if regime:
            matched = self._regime_skills(regime, max_count)
            if matched:
                logger.info("[SkillRouter] regime=%s -> %s", regime, matched)
                return matched

        defaults = get_default_active_skill_ids()
        logger.info("[SkillRouter] using defaults: %s", defaults)
        return defaults[:max_count]

    def _detect_regime(self, ctx: AgentContext) -> Optional[str]:
        for op in ctx.opinions:
            if op.agent_name != "technical":
                continue
            raw = op.raw_data or {}
            ma = str(raw.get("ma_alignment", "")).lower()
            try:
                score = float(raw.get("trend_score", 50))
            except (TypeError, ValueError):
                score = 50.0

            if ma == "bullish" and score >= 70:
                return "trending_up"
            if ma == "bearish" and score <= 30:
                return "trending_down"
            if ma == "neutral" or 35 <= score <= 65:
                return "sideways"
        return None

    def _regime_skills(self, regime: str, max_count: int) -> List[str]:
        try:
            from stopat30m.agent.factory import _SKILL_MANAGER_PROTOTYPE
            if _SKILL_MANAGER_PROTOTYPE is None:
                return []
            for s in _SKILL_MANAGER_PROTOTYPE.list_skills():
                regimes = [r.lower() for r in (s.market_regimes or [])]
                if regime.lower() in regimes:
                    return [s.name]
        except Exception:
            pass
        return []
