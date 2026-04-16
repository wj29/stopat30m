# -*- coding: utf-8 -*-
"""SkillAggregator — weighted aggregation of skill opinions."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from stopat30m.agent.protocols import AgentContext, AgentOpinion
from stopat30m.agent.skills.defaults import extract_skill_id, is_skill_agent_name

logger = logging.getLogger(__name__)

_SIGNAL_SCORES: Dict[str, float] = {
    "strong_buy": 5.0,
    "buy": 4.0,
    "hold": 3.0,
    "sell": 2.0,
    "strong_sell": 1.0,
}

_SCORE_TO_SIGNAL = [
    (4.5, "strong_buy"),
    (3.5, "buy"),
    (2.5, "hold"),
    (1.5, "sell"),
    (0.0, "strong_sell"),
]


class SkillAggregator:
    def aggregate(self, ctx: AgentContext) -> Optional[AgentOpinion]:
        skill_opinions = [op for op in ctx.opinions if is_skill_agent_name(op.agent_name)]
        if not skill_opinions:
            return None

        weights = [op.confidence for op in skill_opinions]
        total_weight = sum(weights) or 1.0

        weighted_score = sum(
            _SIGNAL_SCORES.get(op.signal, 3.0) * w
            for op, w in zip(skill_opinions, weights)
        ) / total_weight

        weighted_confidence = sum(
            op.confidence * w for op, w in zip(skill_opinions, weights)
        ) / total_weight

        final_signal = "hold"
        for threshold, signal in _SCORE_TO_SIGNAL:
            if weighted_score >= threshold:
                final_signal = signal
                break

        names = [extract_skill_id(op.agent_name) or op.agent_name for op in skill_opinions]
        reasoning = f"Skill consensus from {len(skill_opinions)} skills ({', '.join(names)}): score {weighted_score:.2f}/5.0"

        return AgentOpinion(
            agent_name="skill_consensus",
            signal=final_signal,
            confidence=min(1.0, weighted_confidence),
            reasoning=reasoning,
            raw_data={
                "weighted_score": round(weighted_score, 2),
                "skill_count": len(skill_opinions),
                "individual_signals": {
                    op.agent_name: {"signal": op.signal, "confidence": op.confidence}
                    for op in skill_opinions
                },
            },
        )
