# -*- coding: utf-8 -*-
"""
AgentOrchestrator — multi-agent pipeline with configurable modes.

Modes: quick, standard, full, specialist.
Presents the same run() / chat() interface as AgentExecutor.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

from stopat30m.agent.executor import AgentResult
from stopat30m.agent.llm_adapter import LLMToolAdapter
from stopat30m.agent.protocols import (
    AgentContext,
    AgentRunStats,
    StageResult,
    StageStatus,
    normalize_decision_signal,
)
from stopat30m.agent.runner import parse_dashboard_json
from stopat30m.agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    """Multi-agent pipeline orchestrator."""

    def __init__(
        self,
        tool_registry: ToolRegistry,
        llm_adapter: LLMToolAdapter,
        skill_instructions: str = "",
        technical_skill_policy: str = "",
        max_steps: int = 10,
        mode: str = "standard",
        skill_manager: Any = None,
        config: Any = None,
    ):
        self.tool_registry = tool_registry
        self.llm_adapter = llm_adapter
        self.skill_instructions = skill_instructions
        self.technical_skill_policy = technical_skill_policy
        self.max_steps = max_steps
        self.mode = mode
        self.skill_manager = skill_manager
        self.config = config

        timeout_s = 0
        if config:
            timeout_s = getattr(config, "agent_orchestrator_timeout_s", 0)
            if isinstance(config, dict):
                timeout_s = config.get("agent_orchestrator_timeout_s", 0)
        self.timeout_seconds = float(timeout_s) if timeout_s else 180.0

    def run(self, task: str, context: Optional[Dict[str, Any]] = None) -> AgentResult:
        stock_code = (context or {}).get("stock_code", "")
        stock_name = (context or {}).get("stock_name", "")

        ctx = AgentContext(
            query=task,
            stock_code=stock_code,
            stock_name=stock_name,
        )

        if context:
            for k in ("trend_result", "ohlcv", "model_prediction", "news_context",
                       "chip_data", "fundamental_data"):
                if k in context and context[k] is not None:
                    ctx.set_data(k, context[k])

        stats = self._execute_pipeline(ctx)
        return self._build_result(ctx, stats)

    def chat(
        self,
        message: str,
        session_id: str,
        progress_callback: Optional[Callable] = None,
        context: Optional[Dict[str, Any]] = None,
        history: Optional[List[Dict[str, Any]]] = None,
    ) -> AgentResult:
        stock_code = (context or {}).get("stock_code", "")
        stock_name = (context or {}).get("stock_name", "")

        ctx = AgentContext(
            query=message,
            stock_code=stock_code,
            stock_name=stock_name,
            session_id=session_id,
            meta={"response_mode": "chat", "conversation_history": history or []},
        )

        stats = self._execute_pipeline(ctx, progress_callback=progress_callback)
        return self._build_chat_result(ctx, stats)

    _MIN_REMAINING_S = 15.0

    def _execute_pipeline(
        self,
        ctx: AgentContext,
        progress_callback: Optional[Callable] = None,
    ) -> AgentRunStats:
        stats = AgentRunStats()
        start_time = time.time()
        chain = self._build_agent_chain(ctx)

        for agent in chain:
            remaining = self.timeout_seconds - (time.time() - start_time)
            if remaining <= self._MIN_REMAINING_S:
                logger.warning("Orchestrator timeout, skipping %s (%.1fs left)", agent.agent_name, remaining)
                stats.record_stage(StageResult(
                    stage_name=agent.agent_name, status=StageStatus.SKIPPED,
                    error="Orchestrator timeout",
                ))
                continue

            if progress_callback:
                progress_callback({
                    "type": "stage_start",
                    "agent": agent.agent_name,
                    "message": f"正在运行 {agent.agent_name} 分析...",
                })

            result = agent.run(ctx, progress_callback=progress_callback, timeout_seconds=remaining)
            stats.record_stage(result)

            if progress_callback:
                progress_callback({
                    "type": "stage_done",
                    "agent": agent.agent_name,
                    "success": result.success,
                    "duration": result.duration_s,
                })

            if not result.success:
                logger.warning("[%s] failed: %s", agent.agent_name, result.error)

        self._apply_risk_override(ctx)
        return stats

    def _build_agent_chain(self, ctx: AgentContext) -> list:
        from stopat30m.agent.agents.technical_agent import TechnicalAgent
        from stopat30m.agent.agents.intel_agent import IntelAgent
        from stopat30m.agent.agents.risk_agent import RiskAgent
        from stopat30m.agent.agents.decision_agent import DecisionAgent

        common = dict(
            tool_registry=self.tool_registry,
            llm_adapter=self.llm_adapter,
            skill_instructions=self.skill_instructions,
            technical_skill_policy=self.technical_skill_policy,
        )

        if self.mode == "quick":
            return [
                TechnicalAgent(**common),
                DecisionAgent(**common),
            ]
        elif self.mode == "standard":
            return [
                TechnicalAgent(**common),
                IntelAgent(**common),
                DecisionAgent(**common),
            ]
        elif self.mode == "full":
            return [
                TechnicalAgent(**common),
                IntelAgent(**common),
                RiskAgent(**common),
                DecisionAgent(**common),
            ]
        elif self.mode == "specialist":
            chain = [
                TechnicalAgent(**common),
                IntelAgent(**common),
                RiskAgent(**common),
            ]
            skill_agents = self._build_skill_agents(ctx, common)
            chain.extend(skill_agents)
            chain.append(DecisionAgent(**common))
            return chain
        else:
            logger.warning("Unknown mode '%s', falling back to standard", self.mode)
            return [
                TechnicalAgent(**common),
                IntelAgent(**common),
                DecisionAgent(**common),
            ]

    def _build_skill_agents(self, ctx: AgentContext, common: dict) -> list:
        if self.skill_manager is None:
            return []

        try:
            from stopat30m.agent.skills.router import SkillRouter
            router = SkillRouter()
            skill_ids = router.select_skills(ctx, max_count=3)
        except Exception:
            skill_ids = []

        if not skill_ids:
            return []

        agents = []
        try:
            from stopat30m.agent.skills.skill_agent import SkillAgent
            for sid in skill_ids:
                agents.append(SkillAgent(skill_id=sid, **common))
        except Exception as e:
            logger.warning("Failed to build skill agents: %s", e)

        return agents

    @staticmethod
    def _apply_risk_override(ctx: AgentContext) -> None:
        """Downgrade signals when high-severity risk flags exist."""
        high_risk = any(
            rf.get("severity") == "high" for rf in ctx.risk_flags
        )
        if not high_risk:
            return

        for opinion in ctx.opinions:
            if opinion.agent_name == "decision" and opinion.signal in ("buy", "strong_buy"):
                logger.info("Risk override: downgrading decision from %s to hold", opinion.signal)
                opinion.signal = "hold"
                opinion.reasoning += "\n[风险覆盖：高风险标记触发，信号降级为观望]"

        dashboard = ctx.get_data("final_dashboard")
        if isinstance(dashboard, dict):
            dt = dashboard.get("decision_type", "")
            if dt in ("buy", "strong_buy"):
                dashboard["decision_type"] = "hold"
                dashboard["risk_warning"] = (
                    dashboard.get("risk_warning", "") +
                    " ⚠️ 高风险标记触发，信号已降级为观望。"
                )

    def _build_result(self, ctx: AgentContext, stats: AgentRunStats) -> AgentResult:
        dashboard = ctx.get_data("final_dashboard")
        if dashboard:
            return AgentResult(
                success=True,
                content="",
                dashboard=dashboard,
                total_steps=stats.total_stages,
                total_tokens=stats.total_tokens,
                model=", ".join(stats.models_used) if stats.models_used else "",
            )

        raw = ctx.get_data("final_dashboard_raw", "")
        if raw:
            parsed = parse_dashboard_json(raw)
            if parsed:
                return AgentResult(success=True, content=raw, dashboard=parsed,
                                   total_steps=stats.total_stages, total_tokens=stats.total_tokens)

        return AgentResult(
            success=False,
            content=raw,
            error="Pipeline did not produce a dashboard",
            total_steps=stats.total_stages,
            total_tokens=stats.total_tokens,
        )

    @staticmethod
    def _build_chat_result(ctx: AgentContext, stats: AgentRunStats) -> AgentResult:
        text = ctx.get_data("final_response_text", "")
        if text:
            return AgentResult(
                success=True, content=text,
                total_steps=stats.total_stages, total_tokens=stats.total_tokens,
            )

        raw = ""
        for sr in reversed(stats.stage_results):
            if sr.meta.get("raw_text"):
                raw = sr.meta["raw_text"]
                break

        return AgentResult(
            success=bool(raw),
            content=raw,
            error=None if raw else "Pipeline did not produce a response",
            total_steps=stats.total_stages,
            total_tokens=stats.total_tokens,
        )
