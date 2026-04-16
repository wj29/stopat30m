# -*- coding: utf-8 -*-
"""IntelAgent — news & intelligence gathering specialist."""

from __future__ import annotations

import logging
from typing import Optional

from stopat30m.agent.agents.base_agent import BaseAgent
from stopat30m.agent.protocols import AgentContext, AgentOpinion
from stopat30m.agent.runner import try_parse_json

logger = logging.getLogger(__name__)


class IntelAgent(BaseAgent):
    agent_name = "intel"
    max_steps = 4
    tool_names = [
        "search_stock_news",
        "search_comprehensive_intel",
        "get_stock_info",
        "get_capital_flow",
    ]

    def system_prompt(self, ctx: AgentContext) -> str:
        return """\
You are an **Intelligence & Sentiment Agent** specialising in A-shares.

Your task: gather latest news, announcements, risk signals, and capital flow,
then produce a structured JSON opinion.

## Workflow
1. Search latest stock news (earnings, announcements, insider activity)
2. Run comprehensive intel search
3. Call get_capital_flow for main-force capital data
4. Classify catalysts and risk alerts
5. Assess overall sentiment

## Risk Detection Priorities
- Insider sell-downs (减持)
- Earnings warnings (业绩预亏)
- Regulatory penalties
- Industry policy headwinds
- Large lock-up expirations (解禁)
- Sustained main-force capital outflow (主力持续净流出)

## Output Format
Return **only** a JSON object:
{
  "signal": "strong_buy|buy|hold|sell|strong_sell",
  "confidence": 0.0-1.0,
  "reasoning": "2-3 sentence summary",
  "risk_alerts": ["list of detected risks"],
  "positive_catalysts": ["list of catalysts"],
  "sentiment_label": "very_positive|positive|neutral|negative|very_negative",
  "capital_flow_signal": "inflow|outflow|neutral|not_available"
}
"""

    def build_user_message(self, ctx: AgentContext) -> str:
        parts = [f"Gather intelligence and assess sentiment for stock **{ctx.stock_code}**"]
        if ctx.stock_name:
            parts[0] += f" ({ctx.stock_name})"
        parts.append("Use tools to search news and capital flow, then output JSON opinion.")
        return "\n".join(parts)

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        parsed = try_parse_json(raw_text)
        if parsed is None:
            logger.warning("[IntelAgent] failed to parse opinion JSON")
            return None

        ctx.set_data("intel_opinion", parsed)

        for alert in parsed.get("risk_alerts", []):
            if isinstance(alert, str) and alert:
                ctx.add_risk_flag(category="intel", description=alert)

        return AgentOpinion(
            agent_name=self.agent_name,
            signal=parsed.get("signal", "hold"),
            confidence=float(parsed.get("confidence", 0.5)),
            reasoning=parsed.get("reasoning", ""),
            raw_data=parsed,
        )
