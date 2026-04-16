# -*- coding: utf-8 -*-
"""
Shared defaults for trading skills — default policies and skill selection.
"""

from __future__ import annotations

from typing import Dict, List, Optional

SKILL_AGENT_PREFIX = "skill_"

CORE_TRADING_SKILL_POLICY_ZH = """## 默认技能基线（必须严格遵守）

### 1. 严进策略（不追高）
- **绝对不追高**：当股价偏离 MA5 超过 5% 时，坚决不买入
- 乖离率 < 2%：最佳买点区间
- 乖离率 2-5%：可小仓介入
- 乖离率 > 5%：严禁追高！

### 2. 趋势交易（顺势而为）
- MA5 > MA10 > MA20 为多头排列
- 只做多头排列，空头排列坚决不碰

### 3. 效率优先（筹码结构）
- 90%集中度 < 15% 表示筹码集中
- 70-90% 获利盘时警惕获利回吐

### 4. 买点偏好（回踩支撑）
- 最佳买点：缩量回踩 MA5
- 次优买点：回踩 MA10
- 观望：跌破 MA20

### 5. 风险排查重点
- 减持公告、业绩预亏、监管处罚、行业利空、大额解禁

### 6. 估值关注
- PE 明显偏高时需在风险点中说明

### 7. 强势趋势股放宽
- 强势趋势股可适当放宽乖离率要求，设止损
"""

TECHNICAL_SKILL_RULES_EN = """## Default Skill Baseline

- Bullish alignment: MA5 > MA10 > MA20
- Bias from MA5 < 2% -> ideal buy zone; 2-5% -> small position; > 5% -> no chase
- Shrink-pullback to MA5 is preferred entry
- Below MA20 -> hold off
"""


def get_default_trading_skill_policy(*, explicit_skill_selection: bool) -> str:
    if explicit_skill_selection:
        return ""
    return CORE_TRADING_SKILL_POLICY_ZH


def get_default_technical_skill_policy(*, explicit_skill_selection: bool) -> str:
    if explicit_skill_selection:
        return ""
    return TECHNICAL_SKILL_RULES_EN


def get_default_active_skill_ids(
    skills: Optional[list] = None,
    max_count: Optional[int] = None,
) -> List[str]:
    """Return default skill ids to activate."""
    if skills:
        for s in skills:
            if getattr(s, "default_active", False):
                name = getattr(s, "name", "")
                if name:
                    return [name][:max_count] if max_count else [name]

    return ["bull_trend"]


def build_skill_agent_name(skill_id: str) -> str:
    return f"{SKILL_AGENT_PREFIX}{skill_id}"


def extract_skill_id(agent_name: Optional[str]) -> Optional[str]:
    if not agent_name or not isinstance(agent_name, str):
        return None
    if agent_name.startswith(SKILL_AGENT_PREFIX):
        return agent_name[len(SKILL_AGENT_PREFIX):]
    return None


def is_skill_agent_name(agent_name: Optional[str]) -> bool:
    return extract_skill_id(agent_name) is not None
