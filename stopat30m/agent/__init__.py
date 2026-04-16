# -*- coding: utf-8 -*-
"""
Agent module — multi-agent stock analysis with tool calling and skills.

Ported from daily_stock_analysis, adapted for stopat30m data layer.
"""


def __getattr__(name):
    if name == "AgentExecutor":
        from stopat30m.agent.executor import AgentExecutor
        return AgentExecutor
    if name == "AgentResult":
        from stopat30m.agent.executor import AgentResult
        return AgentResult
    if name == "RunLoopResult":
        from stopat30m.agent.runner import RunLoopResult
        return RunLoopResult
    if name in ("AgentContext", "AgentOpinion", "StageResult", "AgentRunStats"):
        from stopat30m.agent import protocols
        return getattr(protocols, name)
    if name == "AgentOrchestrator":
        from stopat30m.agent.orchestrator import AgentOrchestrator
        return AgentOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AgentExecutor",
    "AgentResult",
    "RunLoopResult",
    "AgentContext",
    "AgentOpinion",
    "StageResult",
    "AgentRunStats",
    "AgentOrchestrator",
]
