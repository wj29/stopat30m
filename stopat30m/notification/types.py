"""Analysis result typing for notification report generation."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AnalysisResult(Protocol):
    """Minimal surface used by report generators; extra fields are read via ``getattr``."""

    code: str
    name: str
    sentiment_score: int
    operation_advice: Any
    trend_prediction: Any
    analysis_summary: Any
    technical_analysis: Any
    news_summary: Any
    success: bool
    error_message: Any


def normalize_model_used(model: Any) -> str:
    """Normalize model name string for report footers (ported behavior simplified)."""
    if model is None:
        return ""
    return str(model).strip()
