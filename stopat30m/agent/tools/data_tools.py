# -*- coding: utf-8 -*-
"""
Data tools — adapted for stopat30m data layer.

Tools: get_realtime_quote, get_daily_history, get_chip_distribution,
       get_stock_info, get_capital_flow
"""

import logging

from stopat30m.agent.tools.registry import ToolParameter, ToolDefinition

logger = logging.getLogger(__name__)


def _handle_get_realtime_quote(stock_code: str) -> dict:
    from stopat30m.data.realtime import fetch_spot_prices
    try:
        prices = fetch_spot_prices([stock_code])
        if not prices:
            return {"error": f"No realtime quote for {stock_code}", "retriable": False}
        q = prices[0]
        return {k: v for k, v in q.items() if v is not None}
    except Exception as e:
        return {"error": str(e), "retriable": False}


get_realtime_quote_tool = ToolDefinition(
    name="get_realtime_quote",
    description="Get real-time stock quote including price, change%, volume, PE, PB, market cap.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code, e.g. '600519'"),
    ],
    handler=_handle_get_realtime_quote,
    category="data",
)


def _handle_get_daily_history(stock_code: str, days: int = 60) -> dict:
    from stopat30m.data.sources import fetch_daily_ohlcv
    try:
        df, source = fetch_daily_ohlcv(stock_code)
        if df is None or df.empty:
            return {"error": f"No historical data for {stock_code}"}
        records = df.tail(min(days, len(df))).to_dict(orient="records")
        for r in records:
            if "date" in r:
                r["date"] = str(r["date"])
        return {"code": stock_code, "source": source, "total_records": len(records), "data": records}
    except Exception as e:
        return {"error": str(e)}


get_daily_history_tool = ToolDefinition(
    name="get_daily_history",
    description="Get daily OHLCV historical data with MA indicators. Returns the last N trading days.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code, e.g. '600519'"),
        ToolParameter(name="days", type="integer", description="Number of days (default: 60)", required=False, default=60),
    ],
    handler=_handle_get_daily_history,
    category="data",
)


def _handle_get_chip_distribution(stock_code: str) -> dict:
    try:
        from stopat30m.analysis.chip import fetch_chip_distribution
        chip = fetch_chip_distribution(stock_code)
        if chip is None:
            return {"error": f"No chip data for {stock_code}"}
        return chip
    except Exception as e:
        return {"error": str(e)}


get_chip_distribution_tool = ToolDefinition(
    name="get_chip_distribution",
    description="Get chip distribution analysis: profit ratio, average cost, concentration at 90%/70%.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="A-share stock code, e.g. '600519'"),
    ],
    handler=_handle_get_chip_distribution,
    category="data",
)


def _handle_get_stock_info(stock_code: str) -> dict:
    try:
        from stopat30m.analysis.fundamental import fetch_fundamental_context
        fundamental = fetch_fundamental_context(stock_code)
        if fundamental is None:
            return {"code": stock_code, "info": "No fundamental data available"}
        return {"code": stock_code, "fundamental": fundamental}
    except Exception as e:
        return {"code": stock_code, "error": str(e)}


get_stock_info_tool = ToolDefinition(
    name="get_stock_info",
    description="Get stock fundamental information: valuation, earnings, financial report.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code, e.g. '600519'"),
    ],
    handler=_handle_get_stock_info,
    category="data",
)


def _handle_get_capital_flow(stock_code: str) -> dict:
    try:
        import akshare as ak
        df = ak.stock_individual_fund_flow(stock=stock_code, market="sh" if stock_code.startswith("6") else "sz")
        if df is None or df.empty:
            return {"stock_code": stock_code, "status": "not_available"}
        latest = df.iloc[-1].to_dict()
        return {"stock_code": stock_code, "status": "ok", "data": {str(k): str(v) for k, v in latest.items()}}
    except Exception as e:
        return {"stock_code": stock_code, "status": "error", "error": str(e)}


get_capital_flow_tool = ToolDefinition(
    name="get_capital_flow",
    description="Get main-force capital flow data for an A-share stock.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="A-share stock code, e.g. '600519'"),
    ],
    handler=_handle_get_capital_flow,
    category="data",
)


ALL_DATA_TOOLS = [
    get_realtime_quote_tool,
    get_daily_history_tool,
    get_chip_distribution_tool,
    get_stock_info_tool,
    get_capital_flow_tool,
]
