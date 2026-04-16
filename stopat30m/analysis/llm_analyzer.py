"""LLM-powered deep stock analysis via LiteLLM — Decision Dashboard.

Ported from daily_stock_analysis (DSA) GeminiAnalyzer, adapted for stopat30m.
Supports any provider configured through LiteLLM (OpenAI, Gemini, Anthropic,
DeepSeek, Ollama, AIHubMix, etc.).

Architecture:
  System prompt = A-share market role + trading baseline (7 rules) + dashboard JSON schema
  User prompt   = OHLCV tables + trend + realtime + chip + fundamental + news + task
  Output        = Nested dashboard JSON (core_conclusion, data_perspective, intelligence, battle_plan)
  Quality       = Integrity check -> retry with complement -> placeholder fill
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Optional

import pandas as pd
from loguru import logger

from stopat30m.config import get

from .schemas import DashboardResult, LLMAnalysisResult
from .trend_analyzer import TrendAnalysisResult

# ---------------------------------------------------------------------------
# Trading baseline (ported from DSA CORE_TRADING_SKILL_POLICY_ZH)
# ---------------------------------------------------------------------------

_TRADING_BASELINE = """\
## 默认交易基线（必须严格遵守）

### 1. 严进策略（不追高）
- **绝对不追高**：当股价偏离 MA5 超过 5% 时，坚决不买入
- 乖离率 < 2%：最佳买点区间
- 乖离率 2-5%：可小仓介入
- 乖离率 > 5%：严禁追高！直接判定为"观望"

### 2. 趋势交易（顺势而为）
- **多头排列必须条件**：MA5 > MA10 > MA20
- 只做多头排列的股票，空头排列坚决不碰
- 均线发散上行优于均线粘合

### 3. 效率优先（筹码结构）
- 关注筹码集中度：90%集中度 < 15% 表示筹码集中
- 获利比例分析：70-90% 获利盘时需警惕获利回吐
- 平均成本与现价关系：现价高于平均成本 5-15% 为健康

### 4. 买点偏好（回踩支撑）
- **最佳买点**：缩量回踩 MA5 获得支撑
- **次优买点**：回踩 MA10 获得支撑
- **观望情况**：跌破 MA20 时观望

### 5. 风险排查重点
- 减持公告、业绩预亏、监管处罚、行业政策利空、大额解禁

### 6. 估值关注（PE/PB）
- PE 明显偏高时需在风险点中说明

### 7. 强势趋势股放宽
- 强势趋势股可适当放宽乖离率要求，轻仓追踪但需设止损

### 8. 量化模型预测参考
- 量化模型基于 Alpha158+ 因子训练，预测未来 1-2 日超额收益
- percentile > 80% 且排名 Top 区间 → 模型看多信号，可加权提升 sentiment_score
- percentile < 30% → 模型看空信号，需在风险排查中提及
- 模型信号与技术面一致时可提升 confidence_level
- 模型信号与技术面矛盾时应在 analysis_summary 中说明分歧"""

# ---------------------------------------------------------------------------
# System prompt (ported from DSA SYSTEM_PROMPT, A-share only)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = f"""\
你是一位 A 股投资分析师，负责生成专业的【决策仪表盘】分析报告。

- 本次分析对象为 **A 股**（中国沪深交易所上市股票）。
- 请关注 A 股特有的涨跌停机制（±10%/±20%/±30%）、T+1 交易制度及相关政策因素。

{_TRADING_BASELINE}

## 输出格式：决策仪表盘 JSON

请严格按照以下 JSON 格式输出，这是一个完整的【决策仪表盘】：

```json
{{
    "stock_name": "股票中文名称",
    "sentiment_score": 0-100整数,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "强烈买入/买入/观望/卖出/强烈卖出",
    "decision_type": "strong_buy/buy/wait/sell/strong_sell",
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
                "take_profit_2": "第二止盈位：XX元（可选，趋势较强时设置）"
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

## 评分标准

### 强烈买入（80-100分）：
- ✅ 多头排列 + 乖离率安全 + 量能配合 + 无重大利空
- ✅ 上行空间、入场条件与风险回报清晰
- ✅ 关键风险已排查，仓位与止损计划明确

### 买入（60-79分）：
- ✅ 主信号偏积极，但仍有少量待确认项
- ✅ 允许存在可控风险或次优入场点

### 观望（40-59分）：
- ⚠️ 信号分歧较大，或缺乏足够确认
- ⚠️ 风险与机会大致均衡

### 卖出/减仓（0-39分）：
- ❌ 主要结论转弱，风险明显高于收益
- ❌ 触发了止损/失效条件或重大利空

## 决策仪表盘核心原则

1. **核心结论先行**：一句话说清该买该卖
2. **分持仓建议**：空仓者和持仓者给不同建议
3. **精确狙击点**：必须给出具体价格，不说模糊的话
4. **检查清单可视化**：用 ✅⚠️❌ 明确显示每项检查结果
5. **风险优先级**：舆情中的风险点要醒目标出

请只返回 JSON，不要添加任何其他文本或 markdown 标记。"""


# ---------------------------------------------------------------------------
# User prompt builder
# ---------------------------------------------------------------------------


def _fmt_vol(vol: Any) -> str:
    """Format volume into human-readable string."""
    try:
        v = float(vol)
    except (TypeError, ValueError):
        return "N/A"
    if v >= 1e8:
        return f"{v / 1e8:.2f}亿"
    if v >= 1e4:
        return f"{v / 1e4:.0f}万"
    return f"{v:.0f}"


def _fmt_amount(amount: Any) -> str:
    try:
        a = float(amount)
    except (TypeError, ValueError):
        return "N/A"
    if a >= 1e8:
        return f"{a / 1e8:.2f}亿元"
    if a >= 1e4:
        return f"{a / 1e4:.0f}万元"
    return f"{a:.0f}元"


def build_user_prompt(
    technical: TrendAnalysisResult,
    stock_name: str = "",
    ohlcv_df: pd.DataFrame | None = None,
    model_prediction: dict | None = None,
    news_context: str | None = None,
    chip_data: dict | None = None,
    fundamental_data: dict | None = None,
) -> str:
    """Build the full user prompt matching DSA _format_prompt structure."""
    code = technical.code

    prompt = f"""# 决策仪表盘分析请求

## 股票基础信息
| 项目 | 数据 |
|------|------|
| 股票代码 | **{code}** |
| 股票名称 | **{stock_name or code}** |
"""

    # -- Today's OHLCV --
    if ohlcv_df is not None and not ohlcv_df.empty:
        for col in ["open", "close", "high", "low", "volume"]:
            if col in ohlcv_df.columns:
                ohlcv_df[col] = pd.to_numeric(ohlcv_df[col], errors="coerce")
        latest = ohlcv_df.iloc[-1]
        chg_str = ""
        if "change" in latest and pd.notna(latest["change"]):
            chg_str = f"{latest['change']:+.2f}%"
        prompt += f"""
---

## 技术面数据

### 今日行情
| 指标 | 数值 |
|------|------|
| 收盘价 | {latest['close']:.2f} 元 |
| 开盘价 | {latest['open']:.2f} 元 |
| 最高价 | {latest['high']:.2f} 元 |
| 最低价 | {latest['low']:.2f} 元 |
| 涨跌幅 | {chg_str or 'N/A'} |
| 成交量 | {_fmt_vol(latest['volume'])} |
"""
        # Recent K-lines
        recent_5 = ohlcv_df.tail(5)
        if len(recent_5) >= 3:
            lines = []
            for _, r in recent_5.iterrows():
                c = f" {r['change']:+.2f}%" if "change" in r and pd.notna(r["change"]) else ""
                lines.append(
                    f"| {r['date']} | {r['open']:.2f} | {r['high']:.2f} | "
                    f"{r['low']:.2f} | {r['close']:.2f} | {_fmt_vol(r['volume'])} | {c} |"
                )
            prompt += f"""
### 近期K线
| 日期 | 开 | 高 | 低 | 收 | 量 | 涨跌 |
|------|-----|-----|-----|-----|------|------|
{chr(10).join(lines)}
"""
        # Price range
        recent_20 = ohlcv_df.tail(20)
        h20, l20 = recent_20["high"].max(), recent_20["low"].min()
        prompt += f"""
### 价格区间
| 区间 | 最高 | 最低 |
|------|------|------|
| 近20日 | {h20:.2f} | {l20:.2f} |
"""
        if len(ohlcv_df) >= 11:
            chg5 = (ohlcv_df["close"].iloc[-1] / ohlcv_df["close"].iloc[-6] - 1) * 100
            chg10 = (ohlcv_df["close"].iloc[-1] / ohlcv_df["close"].iloc[-11] - 1) * 100
            prompt += f"| 近5日涨跌 | {chg5:+.2f}% | 近10日涨跌 | {chg10:+.2f}% |\n"

    # -- MA system --
    prompt += f"""
### 均线系统（关键判断指标）
| 均线 | 数值 | 说明 |
|------|------|------|
| MA5 | {technical.ma5:.2f} | 短期趋势线 |
| MA10 | {technical.ma10:.2f} | 中短期趋势线 |
| MA20 | {technical.ma20:.2f} | 中期趋势线 |
| MA60 | {technical.ma60:.2f} | 长期趋势线 |
"""

    # -- Trend analysis --
    bias_warning = "🚨 超过5%，严禁追高！" if technical.bias_ma5 > 5 else "✅ 安全范围"
    prompt += f"""
### 趋势分析预判（基于交易理念）
| 指标 | 数值 | 判定 |
|------|------|------|
| 趋势状态 | {technical.trend_status.value} | |
| 均线排列 | {technical.ma_alignment} | MA5>MA10>MA20为多头 |
| 趋势强度 | {technical.trend_strength}/100 | |
| **乖离率(MA5)** | **{technical.bias_ma5:+.2f}%** | {bias_warning} |
| 乖离率(MA10) | {technical.bias_ma10:+.2f}% | |
| 乖离率(MA20) | {technical.bias_ma20:+.2f}% | |
| 量能状态 | {technical.volume_status.value} | {technical.volume_trend} |
| MACD | {technical.macd_status.value} | {technical.macd_signal} |
| RSI | {technical.rsi_status.value} | RSI6={technical.rsi_6:.1f} RSI12={technical.rsi_12:.1f} |
| 系统信号 | {technical.buy_signal.value} | |
| 系统评分 | {technical.signal_score}/100 | |

#### 系统分析理由
**买入理由**：
{chr(10).join('- ' + r for r in technical.signal_reasons) if technical.signal_reasons else '- 无'}

**风险因素**：
{chr(10).join('- ' + r for r in technical.risk_factors) if technical.risk_factors else '- 无'}
"""

    # -- Support / resistance --
    if technical.support_levels or technical.resistance_levels:
        prompt += "\n### 支撑与阻力\n"
        if technical.support_levels:
            prompt += f"- 支撑位：{', '.join(f'{p:.2f}' for p in technical.support_levels)}\n"
        if technical.resistance_levels:
            prompt += f"- 阻力位：{', '.join(f'{p:.2f}' for p in technical.resistance_levels)}\n"

    # -- Realtime / fundamental data (PE/PB/MV) --
    if fundamental_data:
        rt = fundamental_data.get("realtime", {})
        if rt:
            prompt += f"""
### 实时行情增强数据
| 指标 | 数值 |
|------|------|
| 市盈率(动态) | {rt.get('pe_ratio', 'N/A')} |
| 市净率 | {rt.get('pb_ratio', 'N/A')} |
| 总市值 | {_fmt_amount(rt.get('total_mv'))} |
| 流通市值 | {_fmt_amount(rt.get('circ_mv'))} |
| 换手率 | {rt.get('turnover_rate', 'N/A')}% |
"""
        earnings = fundamental_data.get("earnings", {})
        if earnings:
            fr = earnings.get("financial_report", {})
            div = earnings.get("dividend", {})
            if fr or div:
                prompt += f"""
### 财报与分红（价值投资口径）
| 指标 | 数值 |
|------|------|
| 最近报告期 | {fr.get('report_date', 'N/A')} |
| 营业收入 | {fr.get('revenue', 'N/A')} |
| 归母净利润 | {fr.get('net_profit_parent', 'N/A')} |
| ROE | {fr.get('roe', 'N/A')} |
| TTM股息率 | {div.get('ttm_dividend_yield_pct', 'N/A')} |

> 若上述字段为 N/A，请明确写"数据缺失，无法判断"，禁止编造。
"""

    # -- Chip distribution --
    if chip_data:
        profit_ratio = chip_data.get("profit_ratio", 0)
        prompt += f"""
### 筹码分布数据（效率指标）
| 指标 | 数值 | 健康标准 |
|------|------|----------|
| **获利比例** | **{profit_ratio:.1%}** | 70-90%时警惕 |
| 平均成本 | {chip_data.get('avg_cost', 'N/A')} 元 | 现价应高于5-15% |
| 90%筹码集中度 | {chip_data.get('concentration_90', 'N/A')} | <15%为集中 |
| 70%筹码集中度 | {chip_data.get('concentration_70', 'N/A')} | |
| 筹码状态 | {chip_data.get('chip_status', 'N/A')} | |
"""

    # -- Model prediction --
    if model_prediction:
        mt = model_prediction.get("model_type", "lgbm").upper()
        prompt += f"\n### 量化模型预测（{mt} Alpha 因子模型）\n"
        prompt += "| 指标 | 数值 | 解读 |\n|------|------|------|\n"
        score = model_prediction.get("score")
        pct = model_prediction.get("percentile")
        rank = model_prediction.get("rank_in_pool")
        pool = model_prediction.get("pool_size")
        top_n = model_prediction.get("top_n", False)
        top_k = model_prediction.get("top_k", 10)
        pred_date = model_prediction.get("prediction_date", "N/A")
        if score is not None:
            prompt += f"| 预测分数 | {score:.4f} | 预期超额收益率 |\n"
        if rank is not None and pool:
            rank_pct = rank / pool * 100
            top_label = f"，属于 Top{top_k} 推荐区间" if top_n else ""
            prompt += f"| 全市场排名 | {rank} / {pool} | 前 {rank_pct:.0f}%{top_label} |\n"
        if pct is not None:
            prompt += f"| 分位水平 | {pct:.1%} | 高于 {pct:.0%} 的股票 |\n"
        prompt += f"| 预测日期 | {pred_date} | 基于最新可用数据 |\n"
        if top_n:
            prompt += f"\n> 该股被量化模型列入 Top{top_k} 推荐名单，模型认为短期存在正向超额收益机会。\n"
        elif pct is not None and pct < 0.3:
            prompt += "\n> 该股量化模型评分偏低，模型预期短期表现弱于市场平均。\n"
    else:
        prompt += "\n### 量化模型预测\n量化模型预测：暂无可用预测（未训练或未缓存）\n"

    # -- News / intel --
    prompt += "\n---\n\n## 舆情情报\n"
    if news_context:
        prompt += f"""
以下是 **{stock_name or code}({code})** 近期的新闻搜索结果，请重点提取：
1. 🚨 **风险警报**：减持、处罚、利空
2. 🎯 **利好催化**：业绩、合同、政策
3. 📊 **业绩预期**：年报预告、业绩快报

```
{news_context}
```
"""
    else:
        prompt += "\n未搜索到该股票近期的相关新闻。请主要依据技术面数据进行分析。\n"

    # -- Task instructions --
    prompt += f"""
---

## 分析任务

请为 **{stock_name or code}({code})** 生成【决策仪表盘】，严格按照 JSON 格式输出。

### 重点关注（必须明确回答）：
1. ❓ 是否满足 MA5>MA10>MA20 多头排列？
2. ❓ 当前乖离率是否在安全范围内（<5%）？—— 超过5%必须标注"严禁追高"
3. ❓ 量能是否配合（缩量回调/放量突破）？
4. ❓ 筹码结构是否健康？
5. ❓ 消息面有无重大利空？（减持、处罚、业绩变脸等）

### 决策仪表盘要求：
- **股票名称**：必须输出正确的中文全称
- **核心结论**：一句话说清该买/该卖/该等
- **持仓分类建议**：空仓者怎么做 vs 持仓者怎么做
- **具体狙击点位**：买入价、止损价、止盈价（必须全部给出精确到分的具体数字，不能用"待定"或空值）
- **检查清单**：每项用 ✅/⚠️/❌ 标记

### 输出语言要求（最高优先级）
- 所有 JSON 键名必须保持不变，不要翻译键名。
- `decision_type` 必须保持为 `buy`、`hold`、`sell`。
- 所有面向用户的人类可读文本值必须使用中文。
- 当数据缺失时，请使用中文直接说明"数据缺失，无法判断"。

请输出完整的 JSON 格式决策仪表盘。"""

    return prompt


# ---------------------------------------------------------------------------
# Config key → environment variable mapping
# ---------------------------------------------------------------------------

_KEY_ENV_MAP = {
    "deepseek_api_key": "DEEPSEEK_API_KEY",
    "openai_api_key": "OPENAI_API_KEY",
    "gemini_api_key": "GEMINI_API_KEY",
    "anthropic_api_key": "ANTHROPIC_API_KEY",
    "aihubmix_api_key": "AIHUBMIX_KEY",
}

_BASE_URL_ENV_MAP = {
    "deepseek_base_url": "DEEPSEEK_BASE_URL",
    "openai_base_url": "OPENAI_BASE_URL",
    "ollama_api_base": "OLLAMA_API_BASE",
}

_AIHUBMIX_BASE = "https://aihubmix.com/v1"


# ---------------------------------------------------------------------------
# Integrity check (ported from DSA check_content_integrity)
# ---------------------------------------------------------------------------


def _check_integrity(result: DashboardResult) -> tuple[bool, list[str]]:
    """Validate mandatory fields in the dashboard result."""
    missing: list[str] = []
    if not result.analysis_summary:
        missing.append("analysis_summary")
    if not result.operation_advice:
        missing.append("operation_advice")

    dash = result.dashboard
    core = dash.get("core_conclusion", {})
    if not isinstance(core, dict) or not core.get("one_sentence", "").strip():
        missing.append("dashboard.core_conclusion.one_sentence")

    intel = dash.get("intelligence", {})
    if not isinstance(intel, dict) or "risk_alerts" not in intel:
        missing.append("dashboard.intelligence.risk_alerts")

    if result.decision_type in ("buy", "hold"):
        bp = dash.get("battle_plan", {})
        sp = bp.get("sniper_points", {}) if isinstance(bp, dict) else {}
        if not sp.get("stop_loss", "").strip() if isinstance(sp, dict) else True:
            missing.append("dashboard.battle_plan.sniper_points.stop_loss")

    return len(missing) == 0, missing


def _build_complement_prompt(missing: list[str]) -> str:
    lines = ["### 补全要求：请在上方分析基础上补充以下必填内容，并输出完整 JSON："]
    for f in missing:
        lines.append(f"- `{f}` —— 必填，不能为空")
    return "\n".join(lines)


def _build_retry_prompt(
    base: str, previous: str, missing: list[str],
) -> str:
    complement = _build_complement_prompt(missing)
    return "\n\n".join([
        base,
        "### 上一次输出如下，请在该输出基础上补齐缺失字段，并重新输出完整 JSON。不要省略已有字段：",
        previous,
        complement,
    ])


def _apply_placeholder_fill(result: DashboardResult, missing: list[str]) -> None:
    """Fill missing mandatory fields with safe defaults (in-place)."""
    if "analysis_summary" in missing and not result.analysis_summary:
        result.analysis_summary = "分析数据不足，请结合其他信息综合判断。"
    if "operation_advice" in missing and not result.operation_advice:
        result.operation_advice = "观望"

    dash = result.dashboard
    if "dashboard.core_conclusion.one_sentence" in missing:
        core = dash.setdefault("core_conclusion", {})
        if isinstance(core, dict):
            core.setdefault("one_sentence", "数据不足，建议观望。")

    if "dashboard.intelligence.risk_alerts" in missing:
        intel = dash.setdefault("intelligence", {})
        if isinstance(intel, dict):
            intel.setdefault("risk_alerts", ["暂无风险信息"])

    if "dashboard.battle_plan.sniper_points.stop_loss" in missing:
        bp = dash.setdefault("battle_plan", {})
        if isinstance(bp, dict):
            sp = bp.setdefault("sniper_points", {})
            if isinstance(sp, dict):
                sp.setdefault("stop_loss", "数据不足，请自行设定止损位")


# ---------------------------------------------------------------------------
# LLM Analyzer
# ---------------------------------------------------------------------------


class LLMAnalyzer:
    """LLM-based deep analysis using LiteLLM for provider abstraction.

    Reads all configuration from config.yaml ``llm`` section.
    """

    def __init__(self):
        cfg = get("llm") or {}
        self._model = cfg.get("model", "deepseek/deepseek-chat")
        self._temperature = cfg.get("temperature", 0.3)
        self._max_tokens = cfg.get("max_tokens", 4000)
        self._timeout = cfg.get("timeout", 90)
        self._num_retries = cfg.get("num_retries", 2)
        self._integrity_retries = cfg.get("integrity_retries", 1)
        self._enabled = cfg.get("enabled", True)

        self._extra_params: dict[str, Any] = {}
        self._setup_litellm()
        self._inject_keys(cfg)
        self._resolve_base_urls(cfg)

    @staticmethod
    def _setup_litellm() -> None:
        try:
            import litellm
            litellm.drop_params = True
        except ImportError:
            pass

    def _inject_keys(self, cfg: dict) -> None:
        for cfg_key, env_var in _KEY_ENV_MAP.items():
            value = str(cfg.get(cfg_key, "")).strip()
            if value and not os.environ.get(env_var):
                os.environ[env_var] = value
        for cfg_key, env_var in _BASE_URL_ENV_MAP.items():
            value = str(cfg.get(cfg_key, "")).strip()
            if value and not os.environ.get(env_var):
                os.environ[env_var] = value

    def _resolve_base_urls(self, cfg: dict) -> None:
        model = self._model
        aihubmix_key = str(cfg.get("aihubmix_api_key", "")).strip()
        if aihubmix_key and self._is_openai_compatible(model):
            if not os.environ.get("OPENAI_API_KEY"):
                os.environ["OPENAI_API_KEY"] = aihubmix_key
            self._extra_params["api_base"] = _AIHUBMIX_BASE
            logger.info(f"LLM: using AIHubMix proxy for {model}")
            return
        if model.startswith("deepseek/"):
            base = str(cfg.get("deepseek_base_url", "")).strip()
            if base:
                self._extra_params["api_base"] = base
        elif self._is_openai_compatible(model):
            base = str(cfg.get("openai_base_url", "")).strip()
            if base:
                self._extra_params["api_base"] = base
        elif model.startswith("ollama/"):
            base = str(cfg.get("ollama_api_base", "")).strip() or "http://localhost:11434"
            self._extra_params["api_base"] = base

    @staticmethod
    def _is_openai_compatible(model: str) -> bool:
        return model.startswith("openai/") or "/" not in model

    _TRANSIENT_PATTERNS = (
        "Expecting value: line 1 column 1",
        "LegacyAPIResponse",
        "RemoteDisconnected",
        "Connection aborted",
    )

    def _call_with_retry(self, litellm_mod, kwargs, max_retries: int = 2, delay: float = 2.0):
        """Call litellm.completion with targeted retry for transient proxy errors."""
        for attempt in range(1 + max_retries):
            try:
                return litellm_mod.completion(**kwargs)
            except Exception as e:
                err_str = str(e)
                is_transient = any(pat in err_str for pat in self._TRANSIENT_PATTERNS)
                if is_transient and attempt < max_retries:
                    logger.warning(
                        "LLM transient error (attempt %d/%d), retrying: %s",
                        attempt + 1, max_retries + 1, err_str[:200],
                    )
                    time.sleep(delay)
                    continue
                raise

    @property
    def is_available(self) -> bool:
        if not self._enabled:
            return False
        try:
            import litellm  # noqa: F401
            return True
        except ImportError:
            return False

    def analyze(
        self,
        technical: TrendAnalysisResult,
        model_prediction: dict | None = None,
        ohlcv_df: pd.DataFrame | None = None,
        stock_name: str = "",
        news_context: str | None = None,
        chip_data: dict | None = None,
        fundamental_data: dict | None = None,
    ) -> LLMAnalysisResult:
        if not self.is_available:
            logger.info("LLM analysis unavailable (disabled or litellm not installed)")
            return LLMAnalysisResult(summary="LLM分析不可用", model_used="none")

        import litellm

        user_prompt = build_user_prompt(
            technical,
            stock_name=stock_name,
            ohlcv_df=ohlcv_df,
            model_prediction=model_prediction,
            news_context=news_context,
            chip_data=chip_data,
            fundamental_data=fundamental_data,
        )

        t0 = time.time()
        try:
            kwargs: dict[str, Any] = {
                "model": self._model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": self._temperature,
                "max_tokens": self._max_tokens,
                "timeout": self._timeout,
                "num_retries": self._num_retries,
            }
            kwargs.update(self._extra_params)

            current_prompt = user_prompt
            raw_text = ""
            dashboard_result: DashboardResult | None = None

            for attempt in range(1 + self._integrity_retries):
                if attempt > 0:
                    kwargs["messages"] = [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": current_prompt},
                    ]

                response = self._call_with_retry(litellm, kwargs)
                raw_text = response.choices[0].message.content.strip()

                dashboard_result = self._parse_dashboard(raw_text)
                passed, missing = _check_integrity(dashboard_result)
                if passed:
                    break
                if attempt < self._integrity_retries:
                    logger.info(f"Integrity check failed (missing: {missing}), retrying...")
                    current_prompt = _build_retry_prompt(user_prompt, raw_text, missing)
                else:
                    logger.warning(f"Integrity check failed after retries, filling placeholders: {missing}")
                    _apply_placeholder_fill(dashboard_result, missing)

            elapsed_ms = int((time.time() - t0) * 1000)
            logger.info(f"LLM analysis completed in {elapsed_ms}ms (model={self._model})")

            return self._to_legacy_result(dashboard_result, raw_text, self._model)

        except Exception as e:
            logger.warning(f"LLM analysis failed: {e}")
            return LLMAnalysisResult(
                summary=f"LLM分析失败: {e}",
                model_used=self._model,
                raw_response=str(e),
            )

    def generate_text(
        self,
        prompt: str,
        *,
        max_tokens: int = 8192,
        temperature: float = 0.7,
        system: str | None = None,
    ) -> str:
        """Freeform text generation via LiteLLM (e.g. market daily review)."""
        if not self.is_available:
            return ""
        import litellm

        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "timeout": self._timeout,
            "num_retries": self._num_retries,
        }
        kwargs.update(self._extra_params)
        response = self._call_with_retry(litellm, kwargs)
        return (response.choices[0].message.content or "").strip()

    def _parse_dashboard(self, raw_text: str) -> DashboardResult:
        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        data: dict = {}
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            try:
                import json_repair
                data = json_repair.loads(cleaned)
            except Exception:
                return DashboardResult(analysis_summary="LLM返回格式异常")

        return DashboardResult(
            stock_name=str(data.get("stock_name", "")),
            sentiment_score=int(data.get("sentiment_score", 50)),
            trend_prediction=str(data.get("trend_prediction", "震荡")),
            operation_advice=str(data.get("operation_advice", "观望")),
            decision_type=str(data.get("decision_type", "wait")),
            confidence_level=str(data.get("confidence_level", "中")),
            dashboard=data.get("dashboard", {}),
            analysis_summary=str(data.get("analysis_summary", "")),
            key_points=str(data.get("key_points", "")),
            risk_warning=str(data.get("risk_warning", "")),
            buy_reason=str(data.get("buy_reason", "")),
            trend_analysis=str(data.get("trend_analysis", "")),
            short_term_outlook=str(data.get("short_term_outlook", "")),
            medium_term_outlook=str(data.get("medium_term_outlook", "")),
            technical_analysis=str(data.get("technical_analysis", "")),
            fundamental_analysis=str(data.get("fundamental_analysis", "")),
            news_summary=str(data.get("news_summary", "")),
            data_sources=str(data.get("data_sources", "")),
        )

    @staticmethod
    def _to_legacy_result(
        dash: DashboardResult, raw_text: str, model: str,
    ) -> LLMAnalysisResult:
        """Convert DashboardResult to LLMAnalysisResult for backward compat."""
        d = dash.dashboard or {}
        intel = d.get("intelligence", {}) if isinstance(d, dict) else {}
        bp = d.get("battle_plan", {}) if isinstance(d, dict) else {}
        sp = bp.get("sniper_points", {}) if isinstance(bp, dict) else {}

        def _parse_price(s: Any) -> float | None:
            if s is None:
                return None
            if isinstance(s, (int, float)):
                return float(s)
            import re
            m = re.search(r"[\d.]+", str(s))
            return float(m.group()) if m else None

        sentiment_norm = (dash.sentiment_score / 100) * 2 - 1

        return LLMAnalysisResult(
            sentiment_score=max(-1.0, min(1.0, sentiment_norm)),
            operation_advice=dash.operation_advice,
            confidence={"高": 0.9, "中": 0.6, "低": 0.3}.get(dash.confidence_level, 0.5),
            summary=dash.analysis_summary,
            key_points=[p.strip() for p in dash.key_points.split(",") if p.strip()] if dash.key_points else [],
            risk_warnings=intel.get("risk_alerts", []) if isinstance(intel, dict) else [],
            target_price=_parse_price(sp.get("take_profit")),
            stop_loss_price=_parse_price(sp.get("stop_loss")),
            model_used=model,
            raw_response=raw_text,
            dashboard=dash,
        )
