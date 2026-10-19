from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stopat30m.analysis.market_analyzer import MarketAnalyzer, MarketIndex, MarketOverview


@pytest.mark.parametrize("language", ["zh", "en"])
@pytest.mark.parametrize("llm_available", [True, False])
def test_missing_news_is_disclosed_in_both_prompt_and_final_report(language, llm_available, monkeypatch):
    llm = SimpleNamespace(is_available=llm_available, generate_text=Mock(return_value="# Offline fixture report"))
    market = MarketAnalyzer(analyzer=llm)
    monkeypatch.setattr(market, "_report_language_config", lambda: language)
    overview = MarketOverview(date="2024-01-02", indices=[MarketIndex(code="000001", name="Synthetic index", current=100)])
    result = market.generate_market_review(overview, [])
    disclosure = "未检索到新闻不代表没有事件或风险" if language == "zh" else "missing news does not mean there were no events or risks"
    assert disclosure in result
    if llm_available:
        prompt = llm.generate_text.call_args.args[0]
        assert ("不得编造新闻" if language == "zh" else "Do not invent news") in prompt
    else:
        llm.generate_text.assert_not_called()


def test_available_news_does_not_claim_retrieval_failed(monkeypatch):
    llm = SimpleNamespace(is_available=True, generate_text=Mock(return_value="# Offline fixture report"))
    market = MarketAnalyzer(analyzer=llm)
    monkeypatch.setattr(market, "_report_language_config", lambda: "zh")
    overview = MarketOverview(date="2024-01-02")
    result = market.generate_market_review(overview, [{"title": "Synthetic event", "snippet": "Test evidence"}])
    assert "新闻检索未取得可核实结果" not in result
    assert "Synthetic event" in llm.generate_text.call_args.args[0]
