"""Pydantic schemas for analysis request/response."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AnalysisRequest(BaseModel):
    code: str = Field(..., description="Stock code, e.g. '600519' or 'SH600519'")


# ---------------------------------------------------------------------------
# Dashboard result — nested structure matching DSA decision dashboard
# ---------------------------------------------------------------------------


class DashboardResult(BaseModel):
    """Full nested dashboard output from LLM (DSA-compatible)."""

    stock_name: str = ""
    sentiment_score: int = Field(50, ge=0, le=100)
    trend_prediction: str = "震荡"
    operation_advice: str = "观望"
    decision_type: str = "hold"
    confidence_level: str = "中"

    dashboard: dict[str, Any] = Field(
        default_factory=dict,
        description="Nested dashboard: core_conclusion, data_perspective, intelligence, battle_plan",
    )

    analysis_summary: str = ""
    key_points: str = ""
    risk_warning: str = ""
    buy_reason: str = ""
    trend_analysis: str = ""
    short_term_outlook: str = ""
    medium_term_outlook: str = ""
    technical_analysis: str = ""
    fundamental_analysis: str = ""
    news_summary: str = ""
    data_sources: str = ""


# ---------------------------------------------------------------------------
# LLM analysis result — backward-compatible flat structure
# ---------------------------------------------------------------------------


class LLMAnalysisResult(BaseModel):
    """Structured output from the LLM deep analysis.

    Flat structure kept for backward compatibility with storage and API.
    The full dashboard is available via the ``dashboard`` field.
    """

    sentiment_score: float = Field(0.0, ge=-1.0, le=1.0, description="Sentiment -1 to 1")
    operation_advice: str = Field("观望", description="操作建议: 买入/卖出/持有/观望")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="Confidence 0-1")
    summary: str = Field("", description="Analysis summary in Chinese")
    key_points: list[str] = Field(default_factory=list, description="Key analysis points")
    risk_warnings: list[str] = Field(default_factory=list, description="Risk warnings")
    target_price: float | None = Field(None, description="Target price if applicable")
    stop_loss_price: float | None = Field(None, description="Stop loss price if applicable")
    model_used: str = Field("", description="LLM model identifier")
    raw_response: str = Field("", description="Raw LLM response text")
    dashboard: DashboardResult | None = Field(None, description="Full nested dashboard")


class OHLCVBar(BaseModel):
    """Single OHLCV bar for frontend candlestick chart."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class FullAnalysisResponse(BaseModel):
    """Complete analysis response combining all analysis sources."""
    id: int | None = None
    code: str
    name: str = ""
    analysis_date: str = ""

    # Technical
    signal_score: int = 0
    buy_signal: str = ""
    signal_reasons: list[str] = Field(default_factory=list)
    risk_factors: list[str] = Field(default_factory=list)
    trend_status: str = ""
    technical_detail: dict[str, Any] = Field(default_factory=dict)

    # Model
    model_score: float | None = None
    model_percentile: float | None = None

    # LLM
    llm_sentiment: float | None = None
    llm_operation_advice: str = ""
    llm_confidence: float | None = None
    llm_summary: str = ""
    llm_dashboard: dict[str, Any] = Field(default_factory=dict)

    # OHLCV for chart rendering
    ohlcv: list[OHLCVBar] = Field(default_factory=list, description="Recent OHLCV bars for chart")

    data_source: str = Field("", description="Data source used for OHLCV, e.g. 'baostock', 'qlib'")
    processing_time_ms: int = 0
