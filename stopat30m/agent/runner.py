# -*- coding: utf-8 -*-
"""
Shared runner — ReAct LLM + tool execution loop.

Single authoritative implementation of the agent execution loop.
"""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from stopat30m.agent.llm_adapter import LLMToolAdapter
from stopat30m.agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)

_THINKING_TOOL_LABELS: Dict[str, str] = {
    "get_realtime_quote": "行情获取",
    "get_daily_history": "K线数据获取",
    "analyze_trend": "技术指标分析",
    "get_chip_distribution": "筹码分布分析",
    "search_stock_news": "新闻搜索",
    "search_comprehensive_intel": "综合情报搜索",
    "get_stock_info": "基本信息获取",
    "analyze_pattern": "K线形态识别",
    "get_volume_analysis": "量能分析",
    "calculate_ma": "均线计算",
    "get_capital_flow": "资金流向",
}


@dataclass
class RunLoopResult:
    success: bool = False
    content: str = ""
    tool_calls_log: List[Dict[str, Any]] = field(default_factory=list)
    total_steps: int = 0
    total_tokens: int = 0
    provider: str = ""
    models_used: List[str] = field(default_factory=list)
    error: Optional[str] = None
    messages: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def model(self) -> str:
        return ", ".join(dict.fromkeys(m for m in self.models_used if m))


def serialize_tool_result(result: Any) -> str:
    if result is None:
        return json.dumps({"result": None})
    if isinstance(result, str):
        return result
    if isinstance(result, (dict, list)):
        try:
            return json.dumps(result, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            return str(result)
    if hasattr(result, "__dict__"):
        try:
            d = {k: v for k, v in result.__dict__.items() if not k.startswith("_")}
            return json.dumps(d, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            return str(result)
    return str(result)


def try_parse_json(text: str) -> Optional[Dict[str, Any]]:
    """Best-effort JSON dict extraction from LLM text."""
    if not text:
        return None

    candidates: List[str] = []
    cleaned = text.strip()
    if cleaned:
        candidates.append(cleaned)

    if cleaned.startswith("```"):
        unfenced = re.sub(r'^```(?:json)?\s*', '', cleaned)
        unfenced = re.sub(r'\s*```$', '', unfenced)
        if unfenced:
            candidates.append(unfenced.strip())

    fenced_blocks = re.findall(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    for block in fenced_blocks:
        block = block.strip()
        if block:
            candidates.append(block)

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        snippet = text[start:end + 1].strip()
        if snippet:
            candidates.append(snippet)

    seen: set = set()
    unique: List[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    for candidate in unique:
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, ValueError):
            continue

    try:
        from json_repair import repair_json
    except ImportError:
        repair_json = None

    if repair_json is not None:
        for candidate in unique:
            try:
                repaired = repair_json(candidate)
                obj = json.loads(repaired)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue

    return None


def parse_dashboard_json(content: str) -> Optional[Dict[str, Any]]:
    return try_parse_json(content)


def _remaining_timeout(start: float, budget: Optional[float]) -> Optional[float]:
    if budget is None or budget <= 0:
        return None
    return max(0.0, float(budget) - (time.time() - start))


def run_agent_loop(
    *,
    messages: List[Dict[str, Any]],
    tool_registry: ToolRegistry,
    llm_adapter: LLMToolAdapter,
    max_steps: int = 10,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    thinking_labels: Optional[Dict[str, str]] = None,
    max_wall_clock_seconds: Optional[float] = None,
    tool_call_timeout_seconds: Optional[float] = None,
) -> RunLoopResult:
    """Execute the ReAct LLM <-> tool loop."""
    labels = thinking_labels or _THINKING_TOOL_LABELS
    tool_decls = tool_registry.to_openai_tools()

    start_time = time.time()
    tool_calls_log: List[Dict[str, Any]] = []
    total_tokens = 0
    provider_used = ""
    models_used: List[str] = []
    _MIN_STEP_BUDGET_S = 8.0

    for step in range(max_steps):
        remaining = _remaining_timeout(start_time, max_wall_clock_seconds)
        if remaining is not None and remaining <= 0:
            logger.warning("Agent timed out before step %d", step + 1)
            return RunLoopResult(
                success=False, error=f"Agent timed out ({time.time() - start_time:.1f}s)",
                tool_calls_log=tool_calls_log, total_steps=step,
                total_tokens=total_tokens, provider=provider_used,
                models_used=models_used, messages=messages,
            )
        if remaining is not None and step > 0 and remaining <= _MIN_STEP_BUDGET_S:
            logger.warning("Budget too low for step %d (%.1fs)", step + 1, remaining)
            return RunLoopResult(
                success=False, error=f"Insufficient budget ({remaining:.1f}s remaining)",
                tool_calls_log=tool_calls_log, total_steps=step,
                total_tokens=total_tokens, provider=provider_used,
                models_used=models_used, messages=messages,
            )

        logger.info("Agent step %d/%d", step + 1, max_steps)

        if progress_callback:
            if not tool_calls_log:
                msg = "正在制定分析路径..."
            else:
                last_tool = tool_calls_log[-1].get("tool", "")
                label = labels.get(last_tool, last_tool)
                msg = f"「{label}」已完成，继续深入分析..."
            progress_callback({"type": "thinking", "step": step + 1, "message": msg})

        response = llm_adapter.call_with_tools(messages, tool_decls, timeout=remaining)
        provider_used = response.provider
        total_tokens += (response.usage or {}).get("total_tokens", 0)
        m = response.model or response.provider
        if m and m != "error":
            models_used.append(m)

        if response.tool_calls:
            logger.info("Agent requesting %d tool(s): %s",
                        len(response.tool_calls), [tc.name for tc in response.tool_calls])

            assistant_msg: Dict[str, Any] = {
                "role": "assistant",
                "content": response.content,
                "tool_calls": [
                    {"id": tc.id, "name": tc.name, "arguments": tc.arguments}
                    for tc in response.tool_calls
                ],
            }
            messages.append(assistant_msg)

            tool_results = _execute_tools(
                response.tool_calls, tool_registry, step + 1,
                progress_callback, tool_calls_log,
                tool_call_timeout_seconds,
            )

            tc_order = {tc.id: i for i, tc in enumerate(response.tool_calls)}
            tool_results.sort(key=lambda x: tc_order.get(x["tc"].id, 0))
            for tr in tool_results:
                messages.append({
                    "role": "tool",
                    "name": tr["tc"].name,
                    "tool_call_id": tr["tc"].id,
                    "content": tr["result_str"],
                })
        else:
            logger.info("Agent completed in %d steps (%.1fs, %d tokens)",
                        step + 1, time.time() - start_time, total_tokens)
            if progress_callback:
                progress_callback({"type": "generating", "step": step + 1, "message": "正在生成最终分析..."})

            final_content = response.content or ""
            is_error = response.provider == "error"

            return RunLoopResult(
                success=not is_error and bool(final_content),
                content=final_content if not is_error else "",
                tool_calls_log=tool_calls_log, total_steps=step + 1,
                total_tokens=total_tokens, provider=provider_used,
                models_used=models_used, error=final_content if is_error else None,
                messages=messages,
            )

    logger.warning("Agent hit max steps (%d)", max_steps)
    return RunLoopResult(
        success=False, error=f"Exceeded max steps ({max_steps})",
        tool_calls_log=tool_calls_log, total_steps=max_steps,
        total_tokens=total_tokens, provider=provider_used,
        models_used=models_used, messages=messages,
    )


def _execute_tools(
    tool_calls,
    tool_registry: ToolRegistry,
    step: int,
    progress_callback: Optional[Callable],
    tool_calls_log: List[Dict[str, Any]],
    tool_timeout: Optional[float] = None,
) -> List[Dict[str, Any]]:
    def _exec_single(tc_item):
        t0 = time.time()
        try:
            res = tool_registry.execute(tc_item.name, **tc_item.arguments)
            res_str = serialize_tool_result(res)
            ok = True
        except Exception as e:
            res_str = json.dumps({"error": str(e)})
            ok = False
            logger.warning("Tool '%s' failed: %s", tc_item.name, e)
        dur = round(time.time() - t0, 2)
        return tc_item, res_str, ok, dur

    results: List[Dict[str, Any]] = []

    if len(tool_calls) == 1:
        tc = tool_calls[0]
        if progress_callback:
            progress_callback({"type": "tool_start", "step": step, "tool": tc.name})
        _, result_str, success, dur = _exec_single(tc)
        if progress_callback:
            progress_callback({"type": "tool_done", "step": step, "tool": tc.name, "success": success, "duration": dur})
        tool_calls_log.append({
            "step": step, "tool": tc.name, "arguments": tc.arguments,
            "success": success, "duration": dur, "result_length": len(result_str),
        })
        results.append({"tc": tc, "result_str": result_str})
    else:
        for tc in tool_calls:
            if progress_callback:
                progress_callback({"type": "tool_start", "step": step, "tool": tc.name})

        pool = ThreadPoolExecutor(max_workers=min(len(tool_calls), 5))
        try:
            futures = {pool.submit(_exec_single, tc): tc for tc in tool_calls}
            effective_timeout = tool_timeout if tool_timeout and tool_timeout > 0 else None
            for future in as_completed(futures, timeout=effective_timeout):
                tc_item, result_str, success, dur = future.result()
                if progress_callback:
                    progress_callback({"type": "tool_done", "step": step, "tool": tc_item.name, "success": success, "duration": dur})
                tool_calls_log.append({
                    "step": step, "tool": tc_item.name, "arguments": tc_item.arguments,
                    "success": success, "duration": dur, "result_length": len(result_str),
                })
                results.append({"tc": tc_item, "result_str": result_str})
        except FuturesTimeoutError:
            logger.warning("Tool batch timed out at step %d", step)
            for future, tc_item in futures.items():
                if not future.done():
                    future.cancel()
                    result_str = json.dumps({"error": "Tool timed out"})
                    tool_calls_log.append({
                        "step": step, "tool": tc_item.name, "arguments": tc_item.arguments,
                        "success": False, "duration": 0, "result_length": len(result_str),
                    })
                    results.append({"tc": tc_item, "result_str": result_str})
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    return results
