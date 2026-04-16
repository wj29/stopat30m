# -*- coding: utf-8 -*-
"""
大盘复盘分析模块（A 股专用）

职责：
1. 获取大盘指数数据（上证、深证、创业板等）
2. 搜索市场新闻形成复盘情报
3. 使用大模型生成每日大盘复盘报告
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from stopat30m.analysis.llm_analyzer import LLMAnalyzer
from stopat30m.config import get

_ENGLISH_SECTION_PATTERNS = {
    "market_summary": r"###\s*(?:1\.\s*)?Market Summary",
    "index_commentary": r"###\s*(?:2\.\s*)?(?:Index Commentary|Major Indices)",
    "sector_highlights": r"###\s*(?:4\.\s*)?(?:Sector Highlights|Sector/Theme Highlights)",
}

_CHINESE_SECTION_PATTERNS = {
    "market_summary": r"###\s*一、市场总结",
    "index_commentary": r"###\s*二、(?:指数点评|主要指数)",
    "sector_highlights": r"###\s*四、(?:热点解读|板块表现)",
}


def _normalize_report_language(raw: str | None) -> str:
    v = (raw or "zh").strip().lower()
    return "en" if v in ("en", "english", "us") else "zh"


@dataclass
class MarketIndex:
    """大盘指数数据"""

    code: str
    name: str
    current: float = 0.0
    change: float = 0.0
    change_pct: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    prev_close: float = 0.0
    volume: float = 0.0
    amount: float = 0.0
    amplitude: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "current": self.current,
            "change": self.change,
            "change_pct": self.change_pct,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "amount": self.amount,
            "amplitude": self.amplitude,
        }


@dataclass
class MarketOverview:
    """市场概览数据"""

    date: str
    indices: List[MarketIndex] = field(default_factory=list)
    up_count: int = 0
    down_count: int = 0
    flat_count: int = 0
    limit_up_count: int = 0
    limit_down_count: int = 0
    total_amount: float = 0.0
    top_sectors: List[Dict] = field(default_factory=list)
    bottom_sectors: List[Dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# A-share profile & strategy (from DSA CN_PROFILE / CN_BLUEPRINT)
# ---------------------------------------------------------------------------


@dataclass
class _MarketProfile:
    mood_index_code: str
    news_queries: List[str]
    prompt_index_hint: str
    has_market_stats: bool
    has_sector_rankings: bool


_CN_PROFILE = _MarketProfile(
    mood_index_code="000001",
    news_queries=[
        "A股 大盘 复盘",
        "股市 行情 分析",
        "A股 市场 热点 板块",
    ],
    prompt_index_hint="分析上证、深证、创业板等各指数走势特点",
    has_market_stats=True,
    has_sector_rankings=True,
)


@dataclass(frozen=True)
class _StrategyDimension:
    name: str
    objective: str
    checkpoints: List[str]


@dataclass(frozen=True)
class _MarketStrategyBlueprint:
    title: str
    positioning: str
    principles: List[str]
    dimensions: List[_StrategyDimension]
    action_framework: List[str]

    def to_prompt_block(self) -> str:
        principles_text = "\n".join([f"- {item}" for item in self.principles])
        action_text = "\n".join([f"- {item}" for item in self.action_framework])
        dims = []
        for dim in self.dimensions:
            checkpoints = "\n".join([f"  - {cp}" for cp in dim.checkpoints])
            dims.append(f"- {dim.name}: {dim.objective}\n{checkpoints}")
        dimensions_text = "\n".join(dims)
        return (
            f"## Strategy Blueprint: {self.title}\n"
            f"{self.positioning}\n\n"
            f"### Strategy Principles\n{principles_text}\n\n"
            f"### Analysis Dimensions\n{dimensions_text}\n\n"
            f"### Action Framework\n{action_text}"
        )

    def to_markdown_block(self) -> str:
        dims = "\n".join([f"- **{dim.name}**: {dim.objective}" for dim in self.dimensions])
        return f"### 六、策略框架\n{dims}\n"


_CN_BLUEPRINT = _MarketStrategyBlueprint(
    title="A股市场三段式复盘策略",
    positioning="聚焦指数趋势、资金博弈与板块轮动，形成次日交易计划。",
    principles=[
        "先看指数方向，再看量能结构，最后看板块持续性。",
        "结论必须映射到仓位、节奏与风险控制动作。",
        "判断使用当日数据与近3日新闻，不臆测未验证信息。",
    ],
    dimensions=[
        _StrategyDimension(
            name="趋势结构",
            objective="判断市场处于上升、震荡还是防守阶段。",
            checkpoints=["上证/深证/创业板是否同向", "放量上涨或缩量下跌是否成立", "关键支撑阻力是否被突破"],
        ),
        _StrategyDimension(
            name="资金情绪",
            objective="识别短线风险偏好与情绪温度。",
            checkpoints=["涨跌家数与涨跌停结构", "成交额是否扩张", "高位股是否出现分歧"],
        ),
        _StrategyDimension(
            name="主线板块",
            objective="提炼可交易主线与规避方向。",
            checkpoints=["领涨板块是否具备事件催化", "板块内部是否有龙头带动", "领跌板块是否扩散"],
        ),
    ],
    action_framework=[
        "进攻：指数共振上行 + 成交额放大 + 主线强化。",
        "均衡：指数分化或缩量震荡，控制仓位并等待确认。",
        "防守：指数转弱 + 领跌扩散，优先风控与减仓。",
    ],
)


# ---------------------------------------------------------------------------
# AKShare helpers (from DSA AkshareFetcher, A-share only)
# ---------------------------------------------------------------------------


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "-":
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _is_bse_code(code: str) -> bool:
    c = (code or "").strip().split(".")[0]
    if len(c) != 6 or not c.isdigit():
        return False
    if c.startswith("900"):
        return False
    return c.startswith(("92", "43", "81", "82", "83", "87", "88"))


def _is_kc_cy_stock(code: str) -> bool:
    c = (code or "").strip().split(".")[0]
    return len(c) == 6 and c.isdigit() and (c.startswith("688") or c.startswith("30"))


def _is_st_stock(name: str) -> bool:
    return "ST" in (name or "").upper()


def _pure_code_from_row(code: Any) -> str:
    s = str(code).strip()
    if s.startswith(("SH", "SZ", "sh", "sz")) and len(s) >= 8:
        return s[-6:]
    if "." in s:
        base = s.split(".", 1)[0]
        if len(base) == 6 and base.isdigit():
            return base
    return s


def _fetch_main_indices_ak() -> List[Dict[str, Any]]:
    import akshare as ak

    indices_map = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000688": "科创50",
        "sh000016": "上证50",
        "sh000300": "沪深300",
    }
    results: List[Dict[str, Any]] = []
    try:
        df = ak.stock_zh_index_spot_sina()
        if df is None or df.empty:
            return results
        for code, name in indices_map.items():
            row = df[df["代码"] == code]
            if row.empty:
                row = df[df["代码"].astype(str).str.contains(code, regex=False)]
            if row.empty:
                continue
            r = row.iloc[0]
            current = _safe_float(r.get("最新价", 0))
            prev_close = _safe_float(r.get("昨收", 0))
            high = _safe_float(r.get("最高", 0))
            low = _safe_float(r.get("最低", 0))
            amplitude = 0.0
            if prev_close > 0:
                amplitude = (high - low) / prev_close * 100
            results.append(
                {
                    "code": code,
                    "name": name,
                    "current": current,
                    "change": _safe_float(r.get("涨跌额", 0)),
                    "change_pct": _safe_float(r.get("涨跌幅", 0)),
                    "open": _safe_float(r.get("今开", 0)),
                    "high": high,
                    "low": low,
                    "prev_close": prev_close,
                    "volume": _safe_float(r.get("成交量", 0)),
                    "amount": _safe_float(r.get("成交额", 0)),
                    "amplitude": amplitude,
                }
            )
    except Exception as e:
        logger.error(f"[大盘] AKShare 指数行情失败: {e}")
    return results


def _calc_market_stats(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    code_col = next((c for c in ["代码", "股票代码", "ts_code", "stock_code"] if c in df.columns), None)
    name_col = next((c for c in ["名称", "股票名称", "name"] if c in df.columns), None)
    close_col = next((c for c in ["最新价", "close", "lastPrice"] if c in df.columns), None)
    pre_close_col = next((c for c in ["昨收", "昨日收盘", "pre_close", "lastClose"] if c in df.columns), None)
    amount_col = next((c for c in ["成交额", "amount"] if c in df.columns), None)
    if not all([code_col, name_col, close_col, pre_close_col, amount_col]):
        return None

    limit_up_count = 0
    limit_down_count = 0
    up_count = 0
    down_count = 0
    flat_count = 0

    for code, name, current_price, pre_close, amount in zip(
        df[code_col], df[name_col], df[close_col], df[pre_close_col], df[amount_col]
    ):
        if pd.isna(current_price) or pd.isna(pre_close) or current_price in ["-"] or pre_close in ["-"]:
            continue
        try:
            ap = float(amount) if amount not in (None, "-") else 0.0
        except (TypeError, ValueError):
            ap = 0.0
        if ap == 0:
            continue
        current_price = float(current_price)
        pre_close = float(pre_close)

        pure_code = _pure_code_from_row(code)
        str_name = str(name)

        if _is_bse_code(pure_code):
            ratio = 0.30
        elif _is_kc_cy_stock(pure_code):
            ratio = 0.20
        elif _is_st_stock(str_name):
            ratio = 0.05
        else:
            ratio = 0.10

        limit_up_price = np.floor(pre_close * (1 + ratio) * 100 + 0.5) / 100.0
        limit_down_price = np.floor(pre_close * (1 - ratio) * 100 + 0.5) / 100.0
        limit_up_tol = round(abs(pre_close * (1 + ratio) - limit_up_price), 10)
        limit_down_tol = round(abs(pre_close * (1 - ratio) - limit_down_price), 10)

        if current_price > 0:
            is_limit_up = abs(current_price - limit_up_price) <= limit_up_tol
            is_limit_down = abs(current_price - limit_down_price) <= limit_down_tol
            if is_limit_up:
                limit_up_count += 1
            if is_limit_down:
                limit_down_count += 1
            if current_price > pre_close:
                up_count += 1
            elif current_price < pre_close:
                down_count += 1
            else:
                flat_count += 1

    stats: Dict[str, Any] = {
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "total_amount": 0.0,
    }
    if amount_col in df.columns:
        df2 = df.copy()
        df2[amount_col] = pd.to_numeric(df2[amount_col], errors="coerce")
        stats["total_amount"] = float(df2[amount_col].sum() / 1e8)
    return stats


def _fetch_market_stats_ak() -> Optional[Dict[str, Any]]:
    import akshare as ak

    for fn_name in ("stock_zh_a_spot_em", "stock_zh_a_spot"):
        try:
            df = getattr(ak, fn_name)()
            if df is not None and not df.empty:
                return _calc_market_stats(df)
        except Exception as e:
            logger.warning(f"[大盘] {fn_name} 市场统计失败: {e}")
    return None


def _fetch_sector_rankings_ak(n: int) -> tuple[List[Dict], List[Dict]]:
    import akshare as ak

    def _rank(df: pd.DataFrame, change_col: str, industry_name: str) -> tuple[list, list]:
        df = df.copy()
        df[change_col] = pd.to_numeric(df[change_col], errors="coerce")
        df = df.dropna(subset=[change_col])
        top = df.nlargest(n, change_col)
        top_sectors = [{"name": row[industry_name], "change_pct": row[change_col]} for _, row in top.iterrows()]
        bottom = df.nsmallest(n, change_col)
        bottom_sectors = [{"name": row[industry_name], "change_pct": row[change_col]} for _, row in bottom.iterrows()]
        return top_sectors, bottom_sectors

    try:
        df = ak.stock_board_industry_name_em()
        if df is not None and not df.empty:
            return _rank(df, "涨跌幅", "板块名称")
    except Exception as e:
        logger.warning(f"[大盘] 东财板块排行失败: {e}")
    try:
        df = ak.stock_sector_spot(indicator="行业")
        if df is not None and not df.empty:
            return _rank(df, "涨跌幅", "板块")
    except Exception as e:
        logger.error(f"[大盘] 新浪板块排行失败: {e}")
    return [], []


class MarketAnalyzer:
    """大盘复盘分析器（A 股）。"""

    def __init__(
        self,
        search_service: Any = None,
        analyzer: Optional[LLMAnalyzer] = None,
    ):
        self.search_service = search_service
        self.analyzer = analyzer or LLMAnalyzer()
        self.profile = _CN_PROFILE
        self.strategy = _CN_BLUEPRINT

    def _report_language_config(self) -> str:
        raw = get("analysis", "report_language") or get("project", "report_language") or "zh"
        return _normalize_report_language(str(raw))

    def _get_review_language(self) -> str:
        return self._report_language_config()

    def _get_template_review_language(self) -> str:
        return self._report_language_config()

    def _get_market_scope_name(self, review_language: str | None = None) -> str:
        review_language = review_language or self._get_review_language()
        if review_language == "en":
            return "A-share market"
        return "A股市场"

    def _get_turnover_unit_label(self) -> str:
        return "CNY 100m" if self._get_review_language() == "en" else "亿"

    def _format_turnover_value(self, amount_raw: float) -> str:
        if amount_raw == 0.0:
            return "N/A"
        if amount_raw > 1e6:
            return f"{amount_raw / 1e8:.0f}"
        return f"{amount_raw:.0f}"

    def _get_review_title(self, date: str) -> str:
        if self._get_review_language() == "en":
            return f"## {date} A-share Market Recap"
        return f"## {date} 大盘复盘"

    def _get_index_hint(self) -> str:
        if self._get_review_language() == "en":
            return "Analyze the price action in the SSE, SZSE, ChiNext, and other major indices."
        return self.profile.prompt_index_hint

    def _get_strategy_prompt_block(self) -> str:
        if self._get_review_language() == "en":
            return """## Strategy Blueprint: A-share Three-Phase Recap Strategy
Focus on index trend, liquidity, and sector rotation to shape the next-session trading plan.

### Strategy Principles
- Read index direction first, then confirm liquidity structure, and finally test sector persistence.
- Every conclusion must map to position sizing, trading pace, and risk-control actions.
- Base judgments on today's data and the latest 3-day news flow without inventing unverified information.

### Analysis Dimensions
- Trend Structure: Determine whether the market is in an uptrend, range, or defensive phase.
  - Are the SSE, SZSE, and ChiNext moving in the same direction
  - Is the market advancing on expanding volume or slipping on contracting volume
  - Have key support or resistance levels been reclaimed or broken
- Liquidity & Sentiment: Identify near-term risk appetite and market temperature.
  - Advance/decline breadth and limit-up/limit-down structure
  - Whether turnover is expanding or fading
  - Whether high-beta leaders are showing divergence
- Leading Themes: Distill tradable leadership and areas to avoid.
  - Whether leading sectors have clear event catalysts
  - Whether sector leaders are pulling the group higher
  - Whether weakness is broadening across lagging sectors

### Action Framework
- Offensive: indices rise in sync, turnover expands, and core themes strengthen.
- Balanced: index divergence or low-volume consolidation; keep sizing controlled and wait for confirmation.
- Defensive: indices weaken and laggards broaden; prioritize risk control and de-risking."""
        return self.strategy.to_prompt_block()

    def _get_strategy_markdown_block(self, review_language: str | None = None) -> str:
        review_language = review_language or self._get_review_language()
        if review_language == "en":
            return """### 6. Strategy Framework
- **Trend Structure**: Determine whether the market is in an uptrend, range, or defensive phase.
- **Liquidity & Sentiment**: Track breadth, turnover expansion, and whether leaders are diverging.
- **Leading Themes**: Focus on sectors with catalysts and sustained leadership while avoiding broadening weakness.
"""
        return self.strategy.to_markdown_block()

    def _get_market_mood_text(self, mood_key: str, review_language: str | None = None) -> str:
        review_language = review_language or self._get_review_language()
        if review_language == "en":
            mapping = {
                "strong_up": "strong gains",
                "mild_up": "moderate gains",
                "mild_down": "mild losses",
                "strong_down": "clear weakness",
                "range": "range-bound trading",
            }
        else:
            mapping = {
                "strong_up": "强势上涨",
                "mild_up": "小幅上涨",
                "mild_down": "小幅下跌",
                "strong_down": "明显下跌",
                "range": "震荡整理",
            }
        return mapping[mood_key]

    def get_market_overview(self) -> MarketOverview:
        today = datetime.now().strftime("%Y-%m-%d")
        overview = MarketOverview(date=today)
        overview.indices = self._get_main_indices()
        if self.profile.has_market_stats:
            self._get_market_statistics(overview)
        if self.profile.has_sector_rankings:
            self._get_sector_rankings(overview)
        return overview

    def _get_main_indices(self) -> List[MarketIndex]:
        indices: List[MarketIndex] = []
        try:
            logger.info("[大盘] 获取主要指数实时行情（新浪 spot）...")
            data_list = _fetch_main_indices_ak()
            if data_list:
                for item in data_list:
                    indices.append(
                        MarketIndex(
                            code=item["code"],
                            name=item["name"],
                            current=item["current"],
                            change=item["change"],
                            change_pct=item["change_pct"],
                            open=item["open"],
                            high=item["high"],
                            low=item["low"],
                            prev_close=item["prev_close"],
                            volume=item["volume"],
                            amount=item["amount"],
                            amplitude=item["amplitude"],
                        )
                    )
            if not indices:
                logger.warning("[大盘] 所有行情数据源失败，将依赖新闻搜索进行分析")
            else:
                logger.info(f"[大盘] 获取到 {len(indices)} 个指数行情")
        except Exception as e:
            logger.error(f"[大盘] 获取指数行情失败: {e}")
        return indices

    def _get_market_statistics(self, overview: MarketOverview) -> None:
        try:
            logger.info("[大盘] 获取市场涨跌统计...")
            stats = _fetch_market_stats_ak()
            if stats:
                overview.up_count = stats.get("up_count", 0)
                overview.down_count = stats.get("down_count", 0)
                overview.flat_count = stats.get("flat_count", 0)
                overview.limit_up_count = stats.get("limit_up_count", 0)
                overview.limit_down_count = stats.get("limit_down_count", 0)
                overview.total_amount = stats.get("total_amount", 0.0)
                logger.info(
                    f"[大盘] 涨:{overview.up_count} 跌:{overview.down_count} 平:{overview.flat_count} "
                    f"涨停:{overview.limit_up_count} 跌停:{overview.limit_down_count} "
                    f"成交额:{overview.total_amount:.0f}亿"
                )
        except Exception as e:
            logger.error(f"[大盘] 获取涨跌统计失败: {e}")

    def _get_sector_rankings(self, overview: MarketOverview) -> None:
        try:
            logger.info("[大盘] 获取板块涨跌榜...")
            top_sectors, bottom_sectors = _fetch_sector_rankings_ak(5)
            if top_sectors or bottom_sectors:
                overview.top_sectors = top_sectors
                overview.bottom_sectors = bottom_sectors
                logger.info(f"[大盘] 领涨板块: {[s['name'] for s in overview.top_sectors]}")
                logger.info(f"[大盘] 领跌板块: {[s['name'] for s in overview.bottom_sectors]}")
        except Exception as e:
            logger.error(f"[大盘] 获取板块涨跌榜失败: {e}")

    def search_market_news(self) -> List[Dict]:
        if not self.search_service:
            logger.warning("[大盘] 搜索服务未配置，跳过新闻搜索")
            return []
        all_news: List = []
        search_queries = self.profile.news_queries
        try:
            logger.info("[大盘] 开始搜索市场新闻...")
            market_name = "大盘"
            for query in search_queries:
                response = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name=market_name,
                    max_results=3,
                    focus_keywords=query.split(),
                )
                if response and response.results:
                    all_news.extend(response.results)
                    logger.info(f"[大盘] 搜索 '{query}' 获取 {len(response.results)} 条结果")
            logger.info(f"[大盘] 共获取 {len(all_news)} 条市场新闻")
        except Exception as e:
            logger.error(f"[大盘] 搜索市场新闻失败: {e}")
        return all_news

    def generate_market_review(self, overview: MarketOverview, news: List) -> str:
        if not self.analyzer or not self.analyzer.is_available:
            logger.warning("[大盘] AI分析器未配置或不可用，使用模板生成报告")
            return self._generate_template_review(overview, news)

        prompt = self._build_review_prompt(overview, news)
        logger.info("[大盘] 调用大模型生成复盘报告...")
        review = self.analyzer.generate_text(prompt, max_tokens=8192, temperature=0.7)

        if review:
            logger.info("[大盘] 复盘报告生成成功，长度: %d 字符", len(review))
            return self._inject_data_into_review(review, overview)
        logger.warning("[大盘] 大模型返回为空，使用模板报告")
        return self._generate_template_review(overview, news)

    def _inject_data_into_review(self, review: str, overview: MarketOverview) -> str:
        stats_block = self._build_stats_block(overview)
        indices_block = self._build_indices_block(overview)
        sector_block = self._build_sector_block(overview)
        patterns = _ENGLISH_SECTION_PATTERNS if self._get_review_language() == "en" else _CHINESE_SECTION_PATTERNS

        if stats_block:
            review = self._insert_after_section(review, patterns["market_summary"], stats_block)
        if indices_block:
            review = self._insert_after_section(review, patterns["index_commentary"], indices_block)
        if sector_block:
            review = self._insert_after_section(review, patterns["sector_highlights"], sector_block)
        return review

    @staticmethod
    def _insert_after_section(text: str, heading_pattern: str, block: str) -> str:
        match = re.search(heading_pattern, text)
        if not match:
            return text
        start = match.end()
        next_heading = re.search(r"\n###\s", text[start:])
        if next_heading:
            insert_pos = start + next_heading.start()
        else:
            insert_pos = len(text)
        return text[:insert_pos].rstrip() + "\n\n" + block + "\n\n" + text[insert_pos:].lstrip("\n")

    def _build_stats_block(self, overview: MarketOverview) -> str:
        has_stats = overview.up_count or overview.down_count or overview.total_amount
        if not has_stats:
            return ""
        if self._get_review_language() == "en":
            return (
                f"> 📈 Advancers **{overview.up_count}** / Decliners **{overview.down_count}** / "
                f"Flat **{overview.flat_count}** | "
                f"Limit-up **{overview.limit_up_count}** / Limit-down **{overview.limit_down_count}** | "
                f"Turnover **{overview.total_amount:.0f}** ({self._get_turnover_unit_label()})"
            )
        lines = [
            f"> 📈 上涨 **{overview.up_count}** 家 / 下跌 **{overview.down_count}** 家 / "
            f"平盘 **{overview.flat_count}** 家 | "
            f"涨停 **{overview.limit_up_count}** / 跌停 **{overview.limit_down_count}** | "
            f"成交额 **{overview.total_amount:.0f}** 亿"
        ]
        return "\n".join(lines)

    def _build_indices_block(self, overview: MarketOverview) -> str:
        if not overview.indices:
            return ""
        if self._get_review_language() == "en":
            lines = [
                f"| Index | Last | Change % | Turnover ({self._get_turnover_unit_label()}) |",
                "|-------|------|----------|-----------------|",
            ]
        else:
            lines = [
                "| 指数 | 最新 | 涨跌幅 | 成交额(亿) |",
                "|------|------|--------|-----------|",
            ]
        for idx in overview.indices:
            arrow = "🔴" if idx.change_pct < 0 else "🟢" if idx.change_pct > 0 else "⚪"
            amount_raw = idx.amount or 0.0
            amount_str = self._format_turnover_value(amount_raw)
            lines.append(f"| {idx.name} | {idx.current:.2f} | {arrow} {idx.change_pct:+.2f}% | {amount_str} |")
        return "\n".join(lines)

    def _build_sector_block(self, overview: MarketOverview) -> str:
        if not overview.top_sectors and not overview.bottom_sectors:
            return ""
        lines: List[str] = []
        if overview.top_sectors:
            top = " | ".join([f"**{s['name']}**({s['change_pct']:+.2f}%)" for s in overview.top_sectors[:5]])
            lines.append(f"> 🔥 Leaders: {top}" if self._get_review_language() == "en" else f"> 🔥 领涨: {top}")
        if overview.bottom_sectors:
            bot = " | ".join([f"**{s['name']}**({s['change_pct']:+.2f}%)" for s in overview.bottom_sectors[:5]])
            lines.append(f"> 💧 Laggards: {bot}" if self._get_review_language() == "en" else f"> 💧 领跌: {bot}")
        return "\n".join(lines)

    def _build_review_prompt(self, overview: MarketOverview, news: List) -> str:
        review_language = self._get_review_language()

        indices_text = ""
        for idx in overview.indices:
            direction = "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            indices_text += f"- {idx.name}: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"

        top_sectors_text = ", ".join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.top_sectors[:3]])
        bottom_sectors_text = ", ".join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.bottom_sectors[:3]])

        news_text = ""
        for i, n in enumerate(news[:6], 1):
            if hasattr(n, "title"):
                title = n.title[:50] if n.title else ""
                snippet = n.snippet[:100] if n.snippet else ""
            else:
                title = n.get("title", "")[:50]
                snippet = n.get("snippet", "")[:100]
            news_text += f"{i}. {title}\n   {snippet}\n"

        if review_language == "en":
            if self.profile.has_market_stats:
                stats_block = f"""## Market Breadth
- Advancers: {overview.up_count} | Decliners: {overview.down_count} | Flat: {overview.flat_count}
- Limit-up: {overview.limit_up_count} | Limit-down: {overview.limit_down_count}
- Turnover: {overview.total_amount:.0f} ({self._get_turnover_unit_label()})"""
            else:
                stats_block = "## Market Breadth\n(No equivalent advance/decline statistics are available for this market.)"

            if self.profile.has_sector_rankings:
                sector_block = f"""## Sector Performance
Leading: {top_sectors_text if top_sectors_text else "N/A"}
Lagging: {bottom_sectors_text if bottom_sectors_text else "N/A"}"""
            else:
                sector_block = "## Sector Performance\n(US sector data not available.)"
        else:
            if self.profile.has_market_stats:
                stats_block = f"""## 市场概况
- 上涨: {overview.up_count} 家 | 下跌: {overview.down_count} 家 | 平盘: {overview.flat_count} 家
- 涨停: {overview.limit_up_count} 家 | 跌停: {overview.limit_down_count} 家
- 两市成交额: {overview.total_amount:.0f} 亿元"""
            else:
                stats_block = "## 市场概况\n（美股暂无涨跌家数等统计）"

            if self.profile.has_sector_rankings:
                sector_block = f"""## 板块表现
领涨: {top_sectors_text if top_sectors_text else "暂无数据"}
领跌: {bottom_sectors_text if bottom_sectors_text else "暂无数据"}"""
            else:
                sector_block = "## 板块表现\n（美股暂无板块涨跌数据）"

        data_no_indices_hint = (
            "注意：由于行情数据获取失败，请主要根据【市场新闻】进行定性分析和总结，不要编造具体的指数点位。"
            if not indices_text
            else ""
        )
        if review_language == "en":
            data_no_indices_hint = (
                "Note: Market data fetch failed. Rely mainly on [Market News] for qualitative analysis. Do not invent index levels."
                if not indices_text
                else ""
            )
            indices_placeholder = indices_text if indices_text else "No index data (API error)"
            news_placeholder = news_text if news_text else "No relevant news"
        else:
            indices_placeholder = indices_text if indices_text else "暂无指数数据（接口异常）"
            news_placeholder = news_text if news_text else "暂无相关新闻"

        if review_language == "en":
            report_title = self._get_review_title(overview.date).removeprefix("## ").strip()
            return f"""You are a professional US/A/H market analyst. Please produce a concise market recap report based on the data below.

[Requirements]
- Output pure Markdown only
- No JSON
- No code blocks
- Use emoji sparingly in headings (at most one per heading)
- The entire fixed shell, headings, guidance, and conclusion must be in English

---

# Today's Market Data

## Date
{overview.date}

## Major Indices
{indices_placeholder}

{stats_block}

{sector_block}

## Market News
{news_placeholder}

{data_no_indices_hint}

{self._get_strategy_prompt_block()}

---

# Output Template (follow this structure)

## {report_title}

### 1. Market Summary
(2-3 sentences summarizing overall market tone, index moves, and liquidity.)

### 2. Index Commentary
({self._get_index_hint()})

### 3. Fund Flows
(Interpret what turnover, participation, and flow signals imply.)

### 4. Sector Highlights
(Analyze the drivers behind the leading and lagging sectors or themes.)

### 5. Outlook
(Provide the near-term outlook based on price action and news.)

### 6. Risk Alerts
(List the main risks to monitor.)

### 7. Strategy Plan
(Provide an offensive/balanced/defensive stance, a position-sizing guideline, one invalidation trigger, and end with “For reference only, not investment advice.”)

---

Output the report content directly, no extra commentary.
"""

        return f"""你是一位专业的A/H/美股市场分析师，请根据以下数据生成一份简洁的大盘复盘报告。

【重要】输出要求：
- 必须输出纯 Markdown 文本格式
- 禁止输出 JSON 格式
- 禁止输出代码块
- emoji 仅在标题处少量使用（每个标题最多1个）

---

# 今日市场数据

## 日期
{overview.date}

## 主要指数
{indices_placeholder}

{stats_block}

{sector_block}

## 市场新闻
{news_placeholder}

{data_no_indices_hint}

{self._get_strategy_prompt_block()}

---

# 输出格式模板（请严格按此格式输出）

## {overview.date} 大盘复盘

### 一、市场总结
（2-3句话概括今日市场整体表现，包括指数涨跌、成交量变化）

### 二、指数点评
（{self._get_index_hint()}）

### 三、资金动向
（解读成交额流向的含义）

### 四、热点解读
（分析领涨领跌板块背后的逻辑和驱动因素）

### 五、后市展望
（结合当前走势和新闻，给出明日市场预判）

### 六、风险提示
（需要关注的风险点）

### 七、策略计划
（给出进攻/均衡/防守结论，对应仓位建议，并给出一个触发失效条件；最后补充“建议仅供参考，不构成投资建议”。）

---

请直接输出复盘报告内容，不要输出其他说明文字。
"""

    def _generate_template_review(self, overview: MarketOverview, news: List) -> str:
        template_language = self._get_template_review_language()
        mood_code = self.profile.mood_index_code
        mood_index = next(
            (idx for idx in overview.indices if idx.code == mood_code or idx.code.endswith(mood_code)),
            None,
        )
        if mood_index:
            if mood_index.change_pct > 1:
                market_mood = self._get_market_mood_text("strong_up", template_language)
            elif mood_index.change_pct > 0:
                market_mood = self._get_market_mood_text("mild_up", template_language)
            elif mood_index.change_pct > -1:
                market_mood = self._get_market_mood_text("mild_down", template_language)
            else:
                market_mood = self._get_market_mood_text("strong_down", template_language)
        else:
            market_mood = self._get_market_mood_text("range", template_language)

        indices_text = ""
        for idx in overview.indices[:4]:
            direction = "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            indices_text += f"- **{idx.name}**: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"

        separator = ", " if template_language == "en" else "、"
        top_text = separator.join([s["name"] for s in overview.top_sectors[:3]])
        bottom_text = separator.join([s["name"] for s in overview.bottom_sectors[:3]])

        if template_language == "en":
            stats_section = ""
            if self.profile.has_market_stats:
                stats_section = f"""
### 3. Breadth & Liquidity
| Metric | Value |
|--------|-------|
| Advancers | {overview.up_count} |
| Decliners | {overview.down_count} |
| Limit-up | {overview.limit_up_count} |
| Limit-down | {overview.limit_down_count} |
| Turnover ({self._get_turnover_unit_label()}) | {overview.total_amount:.0f} |
"""
            sector_section = ""
            if self.profile.has_sector_rankings and (top_text or bottom_text):
                sector_section = f"""
### 4. Sector Highlights
- **Leaders**: {top_text or "N/A"}
- **Laggards**: {bottom_text or "N/A"}
"""
            return f"""## {overview.date} A-share Market Recap

### 1. Market Summary
Today's {self._get_market_scope_name(template_language)} showed **{market_mood}**.

### 2. Major Indices
{indices_text or "- No index data available"}
{stats_section}
{sector_section}
### 5. Risk Alerts
Market conditions can change quickly. The data above is for reference only and does not constitute investment advice.

{self._get_strategy_markdown_block(template_language)}

---
*Review Time: {datetime.now().strftime('%H:%M')}*
"""

        stats_section = ""
        if self.profile.has_market_stats:
            stats_section = f"""
### 三、涨跌统计
| 指标 | 数值 |
|------|------|
| 上涨家数 | {overview.up_count} |
| 下跌家数 | {overview.down_count} |
| 涨停 | {overview.limit_up_count} |
| 跌停 | {overview.limit_down_count} |
| 两市成交额 | {overview.total_amount:.0f}亿 |
"""
        sector_section = ""
        if self.profile.has_sector_rankings and (top_text or bottom_text):
            sector_section = f"""
### 四、板块表现
- **领涨**: {top_text}
- **领跌**: {bottom_text}
"""
        strategy_summary = self._get_strategy_markdown_block(template_language)
        return f"""## {overview.date} 大盘复盘

### 一、市场总结
今日A股市场整体呈现**{market_mood}**态势。

### 二、主要指数
{indices_text}
{stats_section}
{sector_section}
### 五、风险提示
市场有风险，投资需谨慎。以上数据仅供参考，不构成投资建议。

{strategy_summary}

---
*复盘时间: {datetime.now().strftime('%H:%M')}*
"""

    def run_daily_review(self) -> str:
        logger.info("========== 开始大盘复盘分析 ==========")
        overview = self.get_market_overview()
        news = self.search_market_news()
        report = self.generate_market_review(overview, news)
        logger.info("========== 大盘复盘分析完成 ==========")
        return report
