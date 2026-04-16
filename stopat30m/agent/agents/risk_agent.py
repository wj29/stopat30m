# -*- coding: utf-8 -*-
"""RiskAgent — dedicated risk screening specialist."""

from __future__ import annotations

import json
import logging
from typing import Optional

from stopat30m.agent.agents.base_agent import BaseAgent
from stopat30m.agent.protocols import AgentContext, AgentOpinion
from stopat30m.agent.runner import try_parse_json

logger = logging.getLogger(__name__)


class RiskAgent(BaseAgent):
    agent_name = "risk"
    max_steps = 4
    tool_names = [
        "search_stock_news",
        "get_realtime_quote",
        "get_stock_info",
    ]

    def system_prompt(self, ctx: AgentContext) -> str:
        return """\
You are a **Risk Screening Agent** focused exclusively on identifying
risks and red flags for the given stock.

## Mandatory Risk Checks
1. **Insider / Major Shareholder Activity** — sell-downs, pledges
2. **Earnings Warnings** — pre-loss, downward revisions
3. **Regulatory** — penalties, investigations
4. **Industry Policy** — headwinds, sector crackdowns
5. **Lock-up Expirations** — large unlocks within 30 days
6. **Valuation Extremes** — PE > 100, PB > 10
7. **Technical Warning Signs** — death crosses, breaking supports

## Output Format
Return **only** a JSON object:
{
  "risk_level": "high|medium|low|none",
  "risk_score": 0-100,
  "flags": [
    {
      "category": "insider|earnings|regulatory|industry|lockup|valuation|technical",
      "severity": "high|medium|low",
      "description": "description of the risk"
    }
  ],
  "veto_buy": true|false,
  "reasoning": "2-3 sentence risk assessment",
  "signal_adjustment": "none|downgrade_one|downgrade_two|veto"
}

Be thorough but factual. Only flag risks backed by evidence.
"""

    def build_user_message(self, ctx: AgentContext) -> str:
        parts = [f"Screen stock **{ctx.stock_code}**"]
        if ctx.stock_name:
            parts[0] += f" ({ctx.stock_name})"
        parts.append("for ALL risk factors listed in your instructions.")

        if ctx.get_data("intel_opinion"):
            parts.append(f"\n[Existing intel data]\n{json.dumps(ctx.get_data('intel_opinion'), ensure_ascii=False, default=str)}")

        return "\n".join(parts)

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        parsed = try_parse_json(raw_text)
        if parsed is None:
            logger.warning("[RiskAgent] failed to parse risk JSON")
            return None

        for flag in parsed.get("flags", []):
            if isinstance(flag, dict):
                ctx.add_risk_flag(
                    category=flag.get("category", "unknown"),
                    description=flag.get("description", ""),
                    severity=flag.get("severity", "medium"),
                )

        return AgentOpinion(
            agent_name=self.agent_name,
            signal=_risk_to_signal(parsed.get("risk_level", "none")),
            confidence=float(parsed.get("risk_score", 50)) / 100.0,
            reasoning=parsed.get("reasoning", ""),
            raw_data=parsed,
        )


def _risk_to_signal(risk_level: str) -> str:
    return {"none": "buy", "low": "hold", "medium": "sell", "high": "strong_sell"}.get(risk_level, "hold")
