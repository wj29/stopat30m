# -*- coding: utf-8 -*-
"""
Search tools — wraps SearchService for agent use.

Tools: search_stock_news, search_comprehensive_intel
"""

import logging

from stopat30m.agent.tools.registry import ToolParameter, ToolDefinition

logger = logging.getLogger(__name__)


def _get_search_service():
    from stopat30m.analysis.search_service import get_search_service
    return get_search_service()


def _handle_search_stock_news(stock_code: str, stock_name: str) -> dict:
    svc = _get_search_service()
    if not svc.is_available:
        return {"error": "No search engine available (no API keys configured)"}

    response = svc.search_stock_news(stock_code, stock_name, max_results=5)
    if not response.success:
        return {"query": response.query, "success": False, "error": response.error_message}

    return {
        "query": response.query,
        "provider": response.provider,
        "success": True,
        "results_count": len(response.results),
        "results": [
            {"title": r.title, "snippet": r.snippet, "url": r.url, "source": r.source}
            for r in response.results
        ],
    }


search_stock_news_tool = ToolDefinition(
    name="search_stock_news",
    description="Search for latest news articles about a stock. Returns titles, snippets, sources.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code, e.g. '600519'"),
        ToolParameter(name="stock_name", type="string", description="Stock name in Chinese, e.g. '贵州茅台'"),
    ],
    handler=_handle_search_stock_news,
    category="search",
)


def _handle_search_comprehensive_intel(stock_code: str, stock_name: str) -> dict:
    svc = _get_search_service()
    if not svc.is_available:
        return {"error": "No search engine available (no API keys configured)"}

    try:
        intel_results = svc.search_comprehensive_intel(
            stock_code=stock_code, stock_name=stock_name, max_searches=6,
        )
    except AttributeError:
        response = svc.search_stock_news(stock_code, stock_name, max_results=8)
        if not response.success:
            return {"error": "Comprehensive intel not available"}
        return {
            "report": response.to_context(),
            "dimensions": {"news": {"results_count": len(response.results)}},
        }

    if not intel_results:
        return {"error": "Comprehensive intel returned no results"}

    report = svc.format_intel_report(intel_results, stock_name)
    dimensions = {}
    for dim_name, response in intel_results.items():
        if response and response.success:
            dimensions[dim_name] = {
                "query": response.query,
                "results_count": len(response.results),
                "results": [
                    {"title": r.title, "snippet": r.snippet, "source": r.source}
                    for r in response.results[:3]
                ],
            }
    return {"report": report, "dimensions": dimensions}


search_comprehensive_intel_tool = ToolDefinition(
    name="search_comprehensive_intel",
    description="Multi-dimensional intelligence: news, risk, earnings outlook, industry trends.",
    parameters=[
        ToolParameter(name="stock_code", type="string", description="Stock code"),
        ToolParameter(name="stock_name", type="string", description="Stock name in Chinese"),
    ],
    handler=_handle_search_comprehensive_intel,
    category="search",
)


ALL_SEARCH_TOOLS = [
    search_stock_news_tool,
    search_comprehensive_intel_tool,
]
