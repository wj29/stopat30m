"""No real searches: missing SDKs, refusal cooldown and secret-safe failures."""
from datetime import datetime
import sys

import pytest
import requests

from stopat30m.analysis import search_service as search

MARKER = "offline-fixture-private-marker"


class Response:
    def __init__(self, status=200, payload=None, *, html=False, retry_after=None):
        self.status_code = status
        self.payload = {"results": []} if payload is None else payload
        self.headers = {"content-type": "text/html" if html else "application/json"}
        if retry_after is not None:
            self.headers["Retry-After"] = retry_after
        self.text = f"<html>Browser verification {MARKER}</html>"
    def json(self):
        if "html" in self.headers["content-type"]:
            raise ValueError(self.text)
        return self.payload


def test_missing_optional_sdks_are_skipped_without_search_or_key_errors(monkeypatch):
    monkeypatch.setitem(sys.modules, "serpapi", None)
    monkeypatch.setitem(sys.modules, "tavily", None)
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: pytest.fail("no provider should send HTTP"))
    service = search.SearchService(serpapi_keys=[MARKER], tavily_keys=[MARKER])
    assert not service.is_available and service._providers == []
    response = service.search_stock_news("market", "大盘")
    assert not response.success and response.results == []
    assert "未取得可核实新闻" in response.to_context()


@pytest.mark.parametrize("status,html", [(403, False), (503, False), (200, True), (302, False)])
def test_searx_failure_stops_one_instance_and_next_query_uses_cooldown(monkeypatch, status, html):
    now = [1000.0]
    calls = []
    def request(url, **kwargs):
        calls.append((url, kwargs))
        return Response(status, html=html)
    monkeypatch.setattr(requests, "get", request)
    monkeypatch.setattr(search.SearXNGSearchProvider, "_get_public_instances", classmethod(lambda cls: ["https://first.invalid", "https://second.invalid", "https://third.invalid"]))
    provider = search.SearXNGSearchProvider(use_public_instances=True, clock=lambda: now[0])
    response = provider.search("fixture query")
    assert not response.success and len(calls) == 1
    assert calls[0][0] == "https://first.invalid/search"
    assert calls[0][1]["allow_redirects"] is False
    assert MARKER not in response.error_message
    assert not provider.is_available
    assert not provider.search("another query").success
    assert len(calls) == 1


def test_owned_instance_does_not_rotate_after_failure_and_retry_after_is_respected(monkeypatch):
    now = [1000.0]
    calls = []
    def request(url, **kwargs):
        calls.append(url)
        return Response(429, retry_after="3600") if len(calls) == 1 else Response()
    monkeypatch.setattr(requests, "get", request)
    provider = search.SearXNGSearchProvider(["https://owned.invalid", "https://backup.invalid"], clock=lambda: now[0])
    assert not provider.search("fixture").success
    now[0] += 3599
    assert not provider.is_available and len(calls) == 1
    now[0] += 2
    assert provider.is_available
    assert provider.search("fixture").success
    assert calls == ["https://owned.invalid/search", "https://owned.invalid/search"]


def test_searx_valid_owned_json_and_zero_results_explain_missing_evidence(monkeypatch):
    today = datetime.now().date().isoformat()
    payload = {"results": [{"title": "fixture news", "content": "fixture summary", "url": "https://news.invalid/item", "publishedDate": today}]}
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: Response(payload=payload))
    provider = search.SearXNGSearchProvider(["http://localhost:8080"])
    response = provider.search("fixture", max_results=1)
    assert response.success and len(response.results) == 1
    assert response.results[0].published_date == today
    empty = search.SearchResponse("fixture", [], "SearXNG", success=True)
    assert "不能据此认定没有新闻或风险事件" in empty.to_context()
    assert search.SearchService()._providers == []  # Public discovery is opt-in.


@pytest.mark.parametrize("url", ["https://user:password@search.invalid", "https://search.invalid?q=private", "file:///tmp/search"])
def test_searx_rejects_credentials_or_non_http_configuration(url):
    with pytest.raises(ValueError) as exc:
        search.SearXNGSearchProvider([url])
    assert url not in str(exc.value)


def test_search_error_logs_and_responses_never_include_keys_or_upstream_html(monkeypatch):
    messages = []
    sink = search._loguru_logger.add(lambda message: messages.append(str(message)), level="DEBUG")
    try:
        monkeypatch.setattr(search, "_post_with_retry", lambda *args, **kwargs: Response(403))
        provider = search.BochaSearchProvider([MARKER])
        response = provider.search("fixture")
        assert not response.success and "HTTP 403" in response.error_message
        assert not provider.is_available
        assert MARKER not in response.error_message and "<html>" not in response.error_message
        def disconnected(*args, **kwargs):
            raise requests.ConnectionError(f"private request context {MARKER}")
        monkeypatch.setattr(search, "_post_with_retry", disconnected)
        provider = search.BochaSearchProvider([MARKER])
        response = provider.search("fixture")
        assert MARKER not in response.error_message
        assert MARKER not in "".join(messages) and "<html>" not in "".join(messages)
    finally:
        search._loguru_logger.remove(sink)


def test_reset_search_service_reloads_subsequent_settings_without_requests(monkeypatch):
    from stopat30m import config
    settings = {"searxng_base_urls": "http://localhost:8080", "searxng_public_instances_enabled": False}
    monkeypatch.setattr(config, "get", lambda section, key=None, default=None: settings.get(key, default))
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: pytest.fail("initializing is offline"))
    search.reset_search_service()
    first = search.get_search_service()
    assert first.is_available
    settings["searxng_base_urls"] = ""
    assert search.get_search_service() is first
    search.reset_search_service()
    second = search.get_search_service()
    assert second is not first and not second.is_available
    search.reset_search_service()
