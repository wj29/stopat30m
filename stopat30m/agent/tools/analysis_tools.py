# -*- coding: utf-8 -*-
"""
Analysis tools — wraps stopat30m trend analyzer and pattern detection.

Tools: analyze_trend, calculate_ma, get_volume_analysis, analyze_pattern
"""

import logging
from typing import Optional

from stopat30m.agent.tools.registry import ToolParameter, ToolDefinition

logger = logging.getLogger(__name__)


def _handle_analyze_trend(stock_code: str) -> dict:
    from stopat30m.data.sources import fetch_daily_ohlcv
    from stopat30m.analysis.trend_analyzer import StockTrendAnalyzer

    df, _ = fetch_daily_ohlcv(stock_code)
    if df is None or df.empty:
        return {"error": f"No data for trend analysis on {stock_code}"}
    if len(df) < 20:
        return {"error": f"Insufficient data for {stock_code} (need >= 20 days)"}

    analyzer = StockTrendAnalyzer()
    try:
        r = analyzer.analyze(df, stock_code)
    except Exception as e:
        return {"error": f"Trend analysis failed: {e}"}

    return {
        "code": r.code,
        "trend_status": r.trend_status.value,
        "ma_alignment": r.ma_alignment,
        "trend_strength": r.trend_strength,
        "ma5": r.ma5, "ma10": r.ma10, "ma20": r.ma20, "ma60": r.ma60,
        "current_price": r.current_price,
        "bias_ma5": round(r.bias_ma5, 2),
        "bias_ma10": round(r.bias_ma10, 2),
        "bias_ma20": round(r.bias_ma20, 2),
        "volume_status": r.volume_status.value,
        "volume_ratio_5d": round(r.volume_ratio_5d, 2),
        "volume_trend": r.volume_trend,
        "macd_dif": round(r.macd_dif, 4),
        "macd_dea": round(r.macd_dea, 4),
        "macd_bar": round(r.macd_bar, 4),
        "macd_status": r.macd_status.value,
        "macd_signal": r.macd_signal,
        "rsi_6": round(r.rsi_6, 2),
        "rsi_12": round(r.rsi_12, 2),
        "rsi_24": round(r.rsi_24, 2),
        "rsi_status": r.rsi_status.value,
        "buy_signal": r.buy_signal.value,
        "signal_score": r.signal_score,
        "signal_reasons": r.signal_reasons,
        "risk_factors": r.risk_factors,
        "support_levels": r.support_levels,
        "resistance_levels": r.resistance_levels,
    }


analyze_trend_tool = ToolDefinition(
    name="analyze_trend",
    description="Run comprehensive technical trend analysis: MA alignment, bias, MACD, RSI, volume, support/resistance, buy/sell signal with score.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code, e.g. '600519'"),
    ],
    handler=_handle_analyze_trend,
    category="analysis",
)


def _handle_calculate_ma(stock_code: str, periods: Optional[str] = None, days: int = 120) -> dict:
    import pandas as pd
    from stopat30m.data.sources import fetch_daily_ohlcv

    df, source = fetch_daily_ohlcv(stock_code)
    if df is None or df.empty:
        return {"error": f"No historical data for {stock_code}"}

    default_periods = [5, 10, 20, 30, 60, 120, 250]
    if periods:
        try:
            period_list = sorted({int(p.strip()) for p in periods.split(",") if p.strip().isdigit()})
        except Exception:
            period_list = default_periods
    else:
        period_list = default_periods

    close = df["close"].tail(days)
    current_price = float(close.iloc[-1])
    result: dict = {"code": stock_code, "current_price": round(current_price, 2), "ma": {}}

    for period in period_list:
        if len(close) < period:
            result["ma"][f"ma{period}"] = None
            continue
        ma_val = float(close.rolling(window=period).mean().iloc[-1])
        bias = round((current_price - ma_val) / ma_val * 100, 2) if ma_val else None
        result["ma"][f"ma{period}"] = {"value": round(ma_val, 2), "bias_pct": bias, "price_above": current_price > ma_val}

    ma_values = [v for v in result["ma"].values() if v is not None]
    above = sum(1 for v in ma_values if v["price_above"])
    result["ma_alignment"] = "多头排列" if above == len(ma_values) else "空头排列" if above == 0 else f"混合({above}/{len(ma_values)})"
    return result


calculate_ma_tool = ToolDefinition(
    name="calculate_ma",
    description="Calculate moving averages (MA5/10/20/30/60/120/250 or custom). Returns values, bias%, alignment.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code"),
        ToolParameter(name="periods", type="string", description="Comma-separated periods (default: '5,10,20,30,60,120,250')", required=False),
        ToolParameter(name="days", type="integer", description="Days of history (default: 120)", required=False, default=120),
    ],
    handler=_handle_calculate_ma,
    category="analysis",
)


def _handle_get_volume_analysis(stock_code: str, days: int = 30) -> dict:
    import numpy as np
    import pandas as pd
    from stopat30m.data.sources import fetch_daily_ohlcv

    df, source = fetch_daily_ohlcv(stock_code)
    if df is None or df.empty:
        return {"error": f"No data for {stock_code}"}
    df = df.tail(days).copy()
    if len(df) < 5:
        return {"error": f"Insufficient data ({len(df)} days)"}

    close = df["close"]
    volume = df["volume"]
    avg_vol_5 = float(volume.tail(5).mean())
    avg_vol_20 = float(volume.tail(20).mean()) if len(df) >= 20 else avg_vol_5
    latest_vol = float(volume.iloc[-1])
    vol_ratio = round(latest_vol / avg_vol_5, 2) if avg_vol_5 > 0 else None

    price_up = close.diff() > 0
    up_days = df[price_up]
    down_days = df[~price_up]
    avg_up_vol = float(up_days["volume"].mean()) if len(up_days) > 0 else 0
    avg_down_vol = float(down_days["volume"].mean()) if len(down_days) > 0 else 0

    if avg_up_vol > avg_down_vol * 1.3:
        pattern = "量价配合良好（上涨放量、下跌缩量）"
    elif avg_down_vol > avg_up_vol * 1.3:
        pattern = "量价背离（下跌放量、上涨缩量，偏空）"
    else:
        pattern = "量价关系中性"

    return {
        "code": stock_code, "latest_volume": latest_vol,
        "avg_volume_5d": round(avg_vol_5), "avg_volume_20d": round(avg_vol_20),
        "volume_ratio_vs_5d": vol_ratio, "pattern": pattern,
    }


get_volume_analysis_tool = ToolDefinition(
    name="get_volume_analysis",
    description="Analyse volume-price relationship: volume ratios, up vs down day volumes, pattern interpretation.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code"),
        ToolParameter(name="days", type="integer", description="Days to analyse (default: 30)", required=False, default=30),
    ],
    handler=_handle_get_volume_analysis,
    category="analysis",
)


def _handle_analyze_pattern(stock_code: str, days: int = 60) -> dict:
    from stopat30m.data.sources import fetch_daily_ohlcv

    df, source = fetch_daily_ohlcv(stock_code)
    if df is None or df.empty:
        return {"error": f"No data for {stock_code}"}
    df = df.tail(days).copy().reset_index(drop=True)
    if len(df) < 10:
        return {"error": f"Insufficient data ({len(df)} days)"}

    o, h, l, c = df["open"].values, df["high"].values, df["low"].values, df["close"].values
    n = len(c)
    patterns = []

    def body(i): return abs(c[i] - o[i])
    avg_body = sum(body(i) for i in range(n)) / n if n > 0 else 1

    for i in range(max(0, n - 3), n):
        bd = body(i)
        if bd > avg_body * 2.5:
            label = "大阳线" if c[i] > o[i] else "大阴线"
            patterns.append({"pattern": label, "day_offset": -(n - 1 - i), "strength": "强"})

    if n >= 3:
        i = n - 1
        if c[i] > o[i] and c[i - 1] < o[i - 1] and o[i] < c[i - 1] and c[i] > o[i - 1]:
            patterns.append({"pattern": "看涨吞没", "day_offset": -1, "strength": "强"})
        elif c[i] < o[i] and c[i - 1] > o[i - 1] and o[i] > c[i - 1] and c[i] < o[i - 1]:
            patterns.append({"pattern": "看跌吞没", "day_offset": -1, "strength": "强"})

    return {
        "code": stock_code, "patterns_count": len(patterns), "patterns": patterns,
        "summary": "未发现明显形态" if not patterns else "、".join(p["pattern"] for p in patterns),
    }


analyze_pattern_tool = ToolDefinition(
    name="analyze_pattern",
    description="Detect candlestick and chart patterns: engulfing, big candles, etc.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code"),
        ToolParameter(name="days", type="integer", description="Days to scan (default: 60)", required=False, default=60),
    ],
    handler=_handle_analyze_pattern,
    category="analysis",
)


ALL_ANALYSIS_TOOLS = [
    analyze_trend_tool,
    calculate_ma_tool,
    get_volume_analysis_tool,
    analyze_pattern_tool,
]
