# -*- coding: utf-8 -*-
"""DecisionAgent — final synthesis and decision-making specialist."""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from stopat30m.agent.agents.base_agent import BaseAgent
from stopat30m.agent.protocols import AgentContext, AgentOpinion, normalize_decision_signal

logger = logging.getLogger(__name__)


class DecisionAgent(BaseAgent):
    agent_name = "decision"
    max_steps = 3
    tool_names: Optional[List[str]] = []

    @staticmethod
    def _is_chat_mode(ctx: AgentContext) -> bool:
        return ctx.meta.get("response_mode") == "chat"

    def system_prompt(self, ctx: AgentContext) -> str:
        if self._is_chat_mode(ctx):
            return """\
You are a **Decision Synthesis Agent** replying to the user's stock question.

You receive structured opinions from technical, intelligence, risk, and skill stages.
Synthesize into a concise, natural-language answer in Chinese.

Requirements:
- Answer the user's question directly
- Use Markdown when helpful
- Highlight main signal, key reasoning, and major risks
- Do NOT output JSON unless explicitly asked

默认使用中文回答。
"""

        skills = ""
        if self.skill_instructions:
            skills = f"\n## Active Trading Skills\n\n{self.skill_instructions}\n"

        return f"""\
You are a **Decision Synthesis Agent** producing the final Decision Dashboard.

You receive:
1. Structured opinions from Technical, Intel agents
2. Risk flags from Risk Agent
3. Skill evaluation results (if applicable)

Synthesise into a single actionable Decision Dashboard.
{skills}
## Core Principles
1. Core conclusion first — one sentence, ≤30 chars
2. Split advice — different for no-position vs has-position
3. Precise sniper levels — concrete price numbers
4. Checklist visual — ✅⚠️❌ for each checkpoint
5. Risk priority — risk alerts must be prominent

## Signal Weighting
- Technical: ~40%
- Intel/sentiment: ~30%
- Risk flags: ~30% (high-severity caps signal at "hold")
- Skill opinion (if present): 20% weight

## Scoring
- 80-100: buy (high conviction)
- 60-79: buy (mostly positive)
- 40-59: hold (mixed signals)
- 20-39: sell (negative + risk)
- 0-19: sell (major risk + bearish)

## Output Format — Decision Dashboard JSON

请严格按照以下 JSON 格式输出：

```json
{{
    "stock_name": "股票中文名称",
    "sentiment_score": 0-100整数,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "强烈买入/买入/观望/卖出/强烈卖出",
    "decision_type": "buy/hold/sell",
    "confidence_level": "高/中/低",

    "dashboard": {{
        "core_conclusion": {{
            "one_sentence": "一句话核心结论（30字以内，直接告诉用户做什么）",
            "signal_type": "🟢买入信号/🟡持有观望/🔴卖出信号/⚠️风险警告",
            "time_sensitivity": "立即行动/今日内/本周内/不急",
            "position_advice": {{
                "no_position": "空仓者建议：具体操作指引",
                "has_position": "持仓者建议：具体操作指引"
            }}
        }},

        "data_perspective": {{
            "trend_status": {{
                "ma_alignment": "均线排列状态描述",
                "is_bullish": true/false,
                "trend_score": 0-100
            }},
            "price_position": {{
                "current_price": 当前价格数值,
                "ma5": MA5数值,
                "ma10": MA10数值,
                "ma20": MA20数值,
                "bias_ma5": 乖离率百分比数值,
                "bias_status": "安全/警戒/危险",
                "support_level": 支撑位价格,
                "resistance_level": 压力位价格
            }},
            "volume_analysis": {{
                "volume_ratio": 量比数值,
                "volume_status": "放量/缩量/平量",
                "volume_meaning": "量能含义解读"
            }},
            "chip_structure": {{
                "profit_ratio": 获利比例,
                "avg_cost": 平均成本,
                "concentration": 筹码集中度,
                "chip_health": "健康/一般/警惕"
            }}
        }},

        "intelligence": {{
            "latest_news": "【最新消息】近期重要新闻摘要",
            "risk_alerts": ["风险点1：具体描述", "风险点2：具体描述"],
            "positive_catalysts": ["利好1：具体描述", "利好2：具体描述"],
            "earnings_outlook": "业绩预期分析",
            "sentiment_summary": "舆情情绪一句话总结"
        }},

        "battle_plan": {{
            "sniper_points": {{
                "ideal_buy": "理想入场位：XX元（含触发条件）",
                "secondary_buy": "次优入场位：XX元（含触发条件）",
                "stop_loss": "止损位：XX元（含触发条件，必须给出具体数字）",
                "take_profit": "第一止盈位：XX元",
                "take_profit_2": "第二止盈位：XX元（可选）"
            }},
            "position_strategy": {{
                "suggested_position": "建议仓位：X成",
                "entry_plan": "分批建仓策略描述",
                "risk_control": "风控策略描述"
            }},
            "action_checklist": [
                "✅/⚠️/❌ 检查项1：均线排列与趋势",
                "✅/⚠️/❌ 检查项2：入场位置与风险回报",
                "✅/⚠️/❌ 检查项3：量价配合",
                "✅/⚠️/❌ 检查项4：无重大利空",
                "✅/⚠️/❌ 检查项5：仓位与止损计划",
                "✅/⚠️/❌ 检查项6：估值与业绩匹配"
            ]
        }}
    }},

    "analysis_summary": "100字综合分析摘要",
    "key_points": "3-5个核心看点，逗号分隔",
    "risk_warning": "风险提示",
    "buy_reason": "操作理由",
    "trend_analysis": "走势形态分析",
    "short_term_outlook": "短期1-3日展望",
    "medium_term_outlook": "中期1-2周展望",
    "technical_analysis": "技术面综合分析",
    "fundamental_analysis": "基本面分析",
    "news_summary": "新闻摘要",
    "data_sources": "数据来源说明"
}}
```

## 输出语言
- JSON 键名保持不变。
- `decision_type` 必须保持为 `buy|hold|sell`。
- 所有人类可读文本值使用中文。
- `dashboard` 内所有子对象都必须完整填写，不可省略。
"""

    def build_user_message(self, ctx: AgentContext) -> str:
        if self._is_chat_mode(ctx):
            parts = ["# User Question", ctx.query, ""]
            if ctx.stock_name:
                parts.append(f"Stock: {ctx.stock_code} ({ctx.stock_name})")
        else:
            parts = [f"# Synthesis Request for {ctx.stock_code}"]
            if ctx.stock_name:
                parts.append(f"Stock: {ctx.stock_code} ({ctx.stock_name})")
            parts.append("")

        if ctx.opinions:
            parts.append("## Agent Opinions")
            for op in ctx.opinions:
                parts.append(f"\n### {op.agent_name}")
                parts.append(f"Signal: {op.signal} | Confidence: {op.confidence:.2f}")
                parts.append(f"Reasoning: {op.reasoning}")
                if op.key_levels:
                    parts.append(f"Key levels: {json.dumps(op.key_levels)}")
                if op.raw_data:
                    extra = {k: v for k, v in op.raw_data.items()
                             if k not in ("signal", "confidence", "reasoning", "key_levels")}
                    if extra:
                        parts.append(f"Extra: {json.dumps(extra, ensure_ascii=False, default=str)}")
                parts.append("")

        if ctx.risk_flags:
            parts.append("## Risk Flags")
            for rf in ctx.risk_flags:
                parts.append(f"- [{rf.get('severity', 'medium')}] {rf.get('category', '')}: {rf.get('description', '')}")
            parts.append("")

        if self._is_chat_mode(ctx):
            parts.append("Answer in natural language using evidence above. No JSON unless asked.")
        else:
            parts.append("Synthesise into Decision Dashboard JSON.")
        return "\n".join(parts)

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        if self._is_chat_mode(ctx):
            text = (raw_text or "").strip()
            if not text:
                return None
            ctx.set_data("final_response_text", text)
            prior = next((op for op in reversed(ctx.opinions) if op.agent_name != self.agent_name), None)
            return AgentOpinion(
                agent_name=self.agent_name,
                signal=prior.signal if prior else "hold",
                confidence=prior.confidence if prior else 0.5,
                reasoning=text,
                raw_data={"response_mode": "chat"},
            )

        from stopat30m.agent.runner import parse_dashboard_json
        dashboard = parse_dashboard_json(raw_text)
        if dashboard:
            dashboard["decision_type"] = normalize_decision_signal(dashboard.get("decision_type", "hold"))
            ctx.set_data("final_dashboard", dashboard)
            try:
                _score = float(dashboard.get("sentiment_score", 50) or 50)
            except (TypeError, ValueError):
                _score = 50.0
            return AgentOpinion(
                agent_name=self.agent_name,
                signal=dashboard.get("decision_type", "hold"),
                confidence=min(1.0, _score / 100.0),
                reasoning=dashboard.get("analysis_summary", ""),
                raw_data=dashboard,
            )
        else:
            ctx.set_data("final_dashboard_raw", raw_text)
            logger.warning("[DecisionAgent] failed to parse dashboard JSON")
            return None
