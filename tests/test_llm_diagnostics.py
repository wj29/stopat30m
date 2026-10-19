"""Read-only catalog diagnostics with in-memory config and fake HTTP only."""
import json
import sys
from types import ModuleType, SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
import requests

from stopat30m.api.deps import get_db_session
from stopat30m.api.v1.endpoints import llm_diagnostics as diagnostics
from stopat30m.auth.deps import get_current_user

KEY = "offline-fixture-private-credential"
BODY_MARKER = "offline-fixture-upstream-private-body"


class FakeResponse:
    def __init__(self, status=200, payload=None, *, raw=None, chunks=None):
        self.status_code = status
        self.headers = {"Content-Type": "application/json", "Location": f"https://elsewhere.invalid/{KEY}"}
        self.raw = json.dumps({"data": []} if payload is None else payload).encode() if raw is None else raw
        self.chunks = chunks
        self.read_chunks = 0
        self.closed = False
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.closed = True
    def iter_content(self, size):
        assert size == 16384
        pieces = self.chunks if self.chunks is not None else [self.raw]
        for chunk in pieces:
            self.read_chunks += 1
            yield chunk


@pytest.fixture
def setup(monkeypatch):
    config = {"model": "openai/fixture-model", "api_type": "chat_completions", "openai_api_key": KEY}
    options = {"api_base": "https://catalog.invalid/v1", "api_key": KEY}
    # The real transport helper is developed independently. Never import a
    # live provider client or read its actual configuration for these tests.
    helper = ModuleType("stopat30m.analysis.llm_transport")
    helper.llm_request_options = lambda cfg: {"model": str(cfg.get("model", "")).replace("aihubmix/", "openai/"), **options}
    helper.llm_effective_provider = lambda cfg: (cfg["provider"] if cfg.get("provider", "auto") != "auto" else
        (str(cfg.get("model", "")).split("/", 1)[0] if "/" in str(cfg.get("model", "")) else "openai"))
    monkeypatch.setitem(sys.modules, "stopat30m.analysis.llm_transport", helper)
    monkeypatch.setattr(diagnostics, "get", lambda section: config)
    calls = []
    state = {"response": FakeResponse(payload={"data": [{"id": "fixture-model"}]})}
    def request(url, **kwargs):
        calls.append((url, kwargs))
        if isinstance(state["response"], Exception):
            raise state["response"]
        return state["response"]
    monkeypatch.setattr(diagnostics.requests, "get", request)
    monkeypatch.setattr(diagnostics.requests, "post", lambda *args, **kwargs: pytest.fail("Diagnostics must never generate text"))
    app = FastAPI()
    app.include_router(diagnostics.router, prefix="/api/v1")
    app.dependency_overrides[get_db_session] = lambda: None
    return app, config, options, calls, state


def admin_client(setup):
    app = setup[0]
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=1, role="admin")
    return TestClient(app)


def assert_no_private_values(response):
    rendered = json.dumps(response, ensure_ascii=False)
    assert KEY not in rendered
    assert BODY_MARKER not in rendered
    assert "Authorization" not in rendered
    assert "api_key" not in rendered and "api_base" not in rendered


def test_diagnostics_requires_site_authentication_and_admin_role(setup):
    app, _, _, calls, _ = setup
    with TestClient(app) as client:
        assert client.get("/api/v1/system/llm/diagnostics").status_code == 401
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=2, role="user")
        assert client.get("/api/v1/system/llm/diagnostics").status_code == 403
    assert calls == []


@pytest.mark.parametrize("listed", [True, False])
def test_get_models_only_reports_membership_without_generation_or_credentials(setup, listed):
    _, _, _, calls, state = setup
    state["response"] = FakeResponse(payload={"data": [{"id": "fixture-model" if listed else "another-model"}], "secret": KEY})
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == ("ok" if listed else "warning")
    assert payload["model_listed"] is listed
    assert payload["generation_tested"] is False
    assert_no_private_values(payload)
    assert len(calls) == 1 and calls[0][0] == "https://catalog.invalid/v1/models"
    request = calls[0][1]
    assert request["headers"]["Authorization"] == f"Bearer {KEY}"
    assert request["allow_redirects"] is False and request["stream"] is True
    assert request["timeout"] == (5, 10)
    assert state["response"].closed


@pytest.mark.parametrize("status, expected", [(401, "unavailable"), (403, "unavailable"),
    (404, "unsupported"), (405, "unsupported"), (302, "unavailable"), (503, "unavailable")])
def test_upstream_http_errors_and_redirects_are_business_results_without_body_echo(setup, status, expected):
    _, _, _, calls, state = setup
    state["response"] = FakeResponse(status, raw=f"<html>{KEY} {BODY_MARKER}</html>".encode())
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.status_code == 200  # Upstream 401 does not log the website user out.
    result = response.json()
    assert result["status"] == expected and result["generation_tested"] is False
    assert result["models"] == [] and result["model_listed"] is None
    assert_no_private_values(result)
    assert len(calls) == 1 and state["response"].read_chunks == 0 and state["response"].closed


@pytest.mark.parametrize("raw", [b"not-json", b"\xff\xfe", b"[]", b'{"data":{}}', b'{"data":null}',
    f"<html>{KEY} {BODY_MARKER}</html>".encode(), json.dumps({"error": f"{KEY} {BODY_MARKER}"}).encode()])
def test_invalid_model_catalog_json_is_not_echoed(setup, raw):
    state = setup[-1]
    state["response"] = FakeResponse(raw=raw)
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.json()["status"] == "unavailable"
    assert response.json()["models"] == []
    assert_no_private_values(response.json())


def test_actual_catalog_enforces_body_limit_stops_reading_and_closes_stream(setup):
    state = setup[-1]
    response = FakeResponse(chunks=[b"x" * diagnostics._MAX_BYTES, b"x", pytest.fail])
    state["response"] = response
    status, models = diagnostics._model_catalog("https://catalog.invalid/v1/", KEY)
    assert status == 200 and models is None
    assert response.read_chunks == 2 and response.closed


def test_actual_catalog_filters_duplicate_invalid_or_credential_bearing_model_ids(setup):
    state = setup[-1]
    state["response"] = FakeResponse(payload={"data": [
        {"id": "valid/model-1.2:latest"}, {"id": "valid/model-1.2:latest"}, {"id": "fixture-model"},
        {"id": KEY}, {"id": f"prefix-{KEY}"}, {"id": "sk-do-not-return"}, {"id": "<html>private</html>"},
        {"id": "has whitespace"}, {"id": "../bad"}, {"id": "a" * 201}, {"id": 42}, "not-an-object", {},
    ]})
    status, models = diagnostics._model_catalog("https://catalog.invalid/v1", KEY)
    assert status == 200 and models == ["fixture-model", "valid/model-1.2:latest"]
    assert_no_private_values(models)


@pytest.mark.parametrize("base", ["ftp://catalog.invalid/v1", "https:///missing-host", "https://user:private@catalog.invalid/v1",
    "https://catalog.invalid/v1?api_key=private", "https://catalog.invalid/v1#private", "https://catalog.invalid:notaport/v1"])
def test_invalid_service_urls_are_rejected_before_network(setup, base):
    _, _, options, calls, _ = setup
    options["api_base"] = base
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.json()["status"] == "unavailable"
    assert calls == []
    assert base not in response.text
    assert_no_private_values(response.json())


@pytest.mark.parametrize("failure", [requests.Timeout(f"request {KEY} {BODY_MARKER}"),
    requests.ConnectionError(f"private response {KEY} {BODY_MARKER}")])
def test_timeouts_and_connection_errors_have_fixed_safe_messages(setup, failure):
    setup[-1]["response"] = failure
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.status_code == 200 and response.json()["status"] == "unavailable"
    assert response.json()["generation_tested"] is False
    assert_no_private_values(response.json())


def test_unsupported_provider_never_sends_a_catalog_or_generation_request(setup):
    _, config, _, calls, _ = setup
    config["model"] = "anthropic/fixture-model"
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.json()["status"] == "unsupported" and calls == []


def test_explicit_claude_with_bare_model_cannot_send_catalog_to_another_provider(setup):
    _, config, _, calls, _ = setup
    config.update(provider="anthropic", model="fixture-claude-model")
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.json()["status"] == "unsupported"
    assert response.json()["provider"] == "anthropic" and calls == []


def test_explicit_aihubmix_checks_canonical_openai_model_membership(setup):
    _, config, _, calls, state = setup
    config.update(provider="aihubmix", model="aihubmix/fixture-model")
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    assert response.json()["status"] == "ok"
    assert response.json()["provider"] == "aihubmix" and response.json()["model_listed"]
    assert len(calls) == 1


def test_membership_is_not_lost_when_service_returns_more_than_display_limit(setup):
    _, config, _, calls, state = setup
    config["model"] = "openai/zz-selected-model"
    state["response"] = FakeResponse(payload={"data": [
        *[{"id": f"aa-model-{number:04d}"} for number in range(300)], {"id": "zz-selected-model"},
    ]})
    with admin_client(setup) as client:
        response = client.get("/api/v1/system/llm/diagnostics")
    payload = response.json()
    assert payload["status"] == "ok" and payload["model_listed"] is True
    assert len(payload["models"]) <= 256
    assert payload["models_truncated"] is True and "zz-selected-model" in payload["models"]
    assert len(calls) == 1 and payload["generation_tested"] is False
