# -*- coding: utf-8 -*-
"""
AgentExecutor — single-agent mode with full tool access.

Provides ``run()`` for analysis (returns dashboard JSON) and ``chat()``
for free-form conversation with tool calling.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from stopat30m.agent.llm_adapter import LLMToolAdapter
from stopat30m.agent.runner import RunLoopResult, parse_dashboard_json, run_agent_loop
from stopat30m.agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


@dataclass
class AgentResult:
    success: bool = False
    content: str = ""
    dashboard: Optional[Dict[str, Any]] = None
    tool_calls_log: List[Dict[str, Any]] = field(default_factory=list)
    total_steps: int = 0
    total_tokens: int = 0
    provider: str = ""
    model: str = ""
    error: Optional[str] = None


AGENT_SYSTEM_PROMPT = """你是一位 A 股投资分析 Agent，拥有数据工具和可切换交易技能。

## 分析工作流程（必须严格按阶段执行）

**第一阶段 · 行情与K线**（必须先执行）
- 调用 `get_realtime_quote` 获取实时行情
- 调用 `get_daily_history` 获取历史K线

**第二阶段 · 技术与筹码**
- 调用 `analyze_trend` 获取技术指标
- 调用 `get_chip_distribution` 获取筹码分布

**第三阶段 · 情报搜索**
- 调用 `search_stock_news` 搜索新闻

**第四阶段 · 综合分析**
- 基于真实数据输出决策仪表盘 JSON

{default_skill_policy_section}

## 规则
1. **必须调用工具获取真实数据**
2. **应用交易技能**
3. **风险优先**
4. **工具失败处理** — 用已有数据继续分析

{skills_section}

## 输出语言
- JSON 键名不变，`decision_type` 为 `buy|hold|sell`
- 所有文本值使用中文
"""

CHAT_SYSTEM_PROMPT = """你是一位 A 股投资分析 Agent，负责解答用户的股票投资问题。

## 分析工作流程（必须严格按阶段执行）

**第一阶段 · 行情与K线**
- `get_realtime_quote`, `get_daily_history`

**第二阶段 · 技术与筹码**
- `analyze_trend`, `get_chip_distribution`

**第三阶段 · 情报搜索**
- `search_stock_news`

**第四阶段 · 综合分析**

{default_skill_policy_section}

## 规则
1. 必须调用工具获取真实数据
2. 应用交易技能
3. 自由对话，不需要输出 JSON
4. 风险优先

{skills_section}

## 输出语言
默认使用中文回答。
"""


class AgentExecutor:
    """Single-agent executor with full tool calling."""

    def __init__(
        self,
        tool_registry: ToolRegistry,
        llm_adapter: LLMToolAdapter,
        skill_instructions: str = "",
        default_skill_policy: str = "",
        max_steps: int = 10,
        timeout_seconds: Optional[float] = None,
    ):
        self.tool_registry = tool_registry
        self.llm_adapter = llm_adapter
        self.skill_instructions = skill_instructions
        self.default_skill_policy = default_skill_policy
        self.max_steps = max_steps
        self.timeout_seconds = timeout_seconds

    def run(self, task: str, context: Optional[Dict[str, Any]] = None) -> AgentResult:
        skills_section = ""
        if self.skill_instructions:
            skills_section = f"## 激活的交易技能\n\n{self.skill_instructions}"
        default_policy = ""
        if self.default_skill_policy:
            default_policy = f"\n{self.default_skill_policy}\n"

        system_prompt = AGENT_SYSTEM_PROMPT.format(
            default_skill_policy_section=default_policy,
            skills_section=skills_section,
        )

        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": self._build_user_message(task, context)},
        ]

        return self._run_loop(messages, parse_dashboard=True)

    def chat(
        self,
        message: str,
        session_id: str,
        progress_callback: Optional[Callable] = None,
        context: Optional[Dict[str, Any]] = None,
        history: Optional[List[Dict[str, Any]]] = None,
    ) -> AgentResult:
        skills_section = ""
        if self.skill_instructions:
            skills_section = f"## 激活的交易技能\n\n{self.skill_instructions}"
        default_policy = ""
        if self.default_skill_policy:
            default_policy = f"\n{self.default_skill_policy}\n"

        system_prompt = CHAT_SYSTEM_PROMPT.format(
            default_skill_policy_section=default_policy,
            skills_section=skills_section,
        )

        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
        ]
        if history:
            messages.extend(history)

        if context:
            context_parts = []
            if context.get("stock_code"):
                context_parts.append(f"股票代码: {context['stock_code']}")
            if context.get("stock_name"):
                context_parts.append(f"股票名称: {context['stock_name']}")
            if context_parts:
                ctx_msg = "[系统提供的上下文]\n" + "\n".join(context_parts)
                messages.append({"role": "user", "content": ctx_msg})
                messages.append({"role": "assistant", "content": "好的，我已了解。请告诉我你想了解什么？"})

        messages.append({"role": "user", "content": message})

        return self._run_loop(messages, parse_dashboard=False, progress_callback=progress_callback)

    def _run_loop(
        self,
        messages: List[Dict[str, Any]],
        parse_dashboard: bool,
        progress_callback: Optional[Callable] = None,
    ) -> AgentResult:
        loop_result = run_agent_loop(
            messages=messages,
            tool_registry=self.tool_registry,
            llm_adapter=self.llm_adapter,
            max_steps=self.max_steps,
            progress_callback=progress_callback,
            max_wall_clock_seconds=self.timeout_seconds,
        )

        model_str = loop_result.model

        if parse_dashboard and loop_result.success:
            dashboard = parse_dashboard_json(loop_result.content)
            return AgentResult(
                success=dashboard is not None,
                content=loop_result.content,
                dashboard=dashboard,
                tool_calls_log=loop_result.tool_calls_log,
                total_steps=loop_result.total_steps,
                total_tokens=loop_result.total_tokens,
                provider=loop_result.provider,
                model=model_str,
                error=None if dashboard else "Failed to parse dashboard JSON",
            )

        return AgentResult(
            success=loop_result.success,
            content=loop_result.content,
            dashboard=None,
            tool_calls_log=loop_result.tool_calls_log,
            total_steps=loop_result.total_steps,
            total_tokens=loop_result.total_tokens,
            provider=loop_result.provider,
            model=model_str,
            error=loop_result.error,
        )

    @staticmethod
    def _build_user_message(task: str, context: Optional[Dict[str, Any]] = None) -> str:
        parts = [task]
        if context:
            if context.get("stock_code"):
                parts.append(f"\n股票代码: {context['stock_code']}")
            parts.append("输出语言: 中文")
        parts.append("\n请使用可用工具获取数据，然后以决策仪表盘 JSON 格式输出分析结果。")
        return "\n".join(parts)
