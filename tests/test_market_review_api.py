"""Real JWT/SSE access and deterministic market-review jobs; no external IO."""
import builtins
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopat30m.api.deps import get_db_session
from stopat30m.api.v1.endpoints import analysis, auth
from stopat30m.auth import service
from stopat30m.storage.models import Base, User

PREFIX = "/api/v1/analysis/market-review"
TEST_SECRET = "offline-only-review-jwt-signing-key"


@pytest.fixture
def client(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'review.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    with sessions.begin() as db:
        db.add(User(id=1, username="review-owner", password_hash=service.hash_password("offline-password"), role="admin"))
        db.add(User(id=2, username="disabled", password_hash="unused", is_active=False))
    monkeypatch.setattr(service, "_secret_key", TEST_SECRET)
    monkeypatch.setattr(service, "_get_expire_hours", lambda: 1)
    monkeypatch.setattr(analysis, "_review_job", analysis.ReviewJob())
    app = FastAPI()
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(analysis.router, prefix="/api/v1")
    def session():
        with sessions() as db:
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise
    # Override only storage. The real OAuth/JWT/user dependency stays enabled.
    app.dependency_overrides[get_db_session] = session
    with TestClient(app) as browser:
        login = browser.post("/api/v1/auth/login", json={"username": "review-owner", "password": "offline-password"})
        assert login.status_code == 200
        browser.review_token = login.json()["access_token"]
        browser.review_headers = {"Authorization": f"Bearer {browser.review_token}"}
        yield browser
    engine.dispose()


@pytest.fixture
def dependencies(monkeypatch):
    analyzer, search = object(), object()
    market = Mock()
    market.get_market_overview.return_value = {"source": "offline_fixture", "indices": []}
    market.search_market_news.return_value = [{"title": "Synthetic news, no external fetch"}]
    market.generate_market_review.return_value = "离线合成复盘报告，仅用于编排回归。"
    notifier = Mock()
    notifier.is_available.return_value = True
    notifier.send.return_value = True
    constructors = {
        "stopat30m.analysis.llm_analyzer": {"LLMAnalyzer": Mock(return_value=analyzer)},
        "stopat30m.analysis.market_analyzer": {"MarketAnalyzer": Mock(return_value=market)},
        "stopat30m.analysis.search_service": {"get_search_service": Mock(return_value=search)},
        "stopat30m.notification.service": {"NotificationService": Mock(return_value=notifier)},
    }
    for name, values in constructors.items():
        module = ModuleType(name)
        module.__dict__.update(values)
        monkeypatch.setitem(sys.modules, name, module)
    return SimpleNamespace(analyzer=analyzer, search=search, market=market, notifier=notifier, constructors=constructors)


@pytest.fixture
def deferred_threads(monkeypatch):
    pending = []
    class DeferredThread:
        def __init__(self, *, target, args, daemon):
            self.target, self.args, self.daemon = target, args, daemon
            pending.append(self)
        def start(self):
            pass  # Reproduce the OS scheduling gap before the worker executes.
        def run(self):
            self.target(*self.args)
    # Replace only this module's threading namespace, not the global stdlib
    # used by TestClient or ThreadPoolExecutor.
    monkeypatch.setattr(analysis, "threading", SimpleNamespace(Thread=DeferredThread))
    return pending


def event_payload(response):
    return [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]


def test_stream_requires_real_token_and_accepts_jwt_header_or_eventsource_query(client):
    assert client.get(PREFIX + "/stream").status_code == 401
    assert client.get(PREFIX + "/stream", headers={"Authorization": "Bearer invalid"}).status_code == 401
    assert client.get(PREFIX + "/stream", params={"token": "invalid"}).status_code == 401
    assert client.post(PREFIX).status_code == 401
    valid = client.get(PREFIX + "/stream", headers=client.review_headers)
    assert valid.status_code == 200 and valid.headers["content-type"].startswith("text/event-stream")
    assert event_payload(valid)[0]["status"] == "idle"
    query = client.get(PREFIX + "/stream", params={"token": client.review_token})
    assert query.status_code == 200 and event_payload(query)[0]["status"] == "idle"
    expired = jwt.encode({"sub": "1", "exp": datetime.now(timezone.utc) - timedelta(seconds=10)}, TEST_SECRET, algorithm="HS256")
    disabled = service.create_access_token(2, "user", "disabled")
    for token in (expired, disabled):
        assert client.get(PREFIX + "/stream", headers={"Authorization": f"Bearer {token}"}).status_code == 401


def test_running_is_reserved_before_worker_start_and_concurrent_submit_cannot_replace_job(client, deferred_threads):
    together = Barrier(8)
    def submit(_):
        together.wait(timeout=5)
        return client.post(PREFIX, headers=client.review_headers).json()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(8)))
    assert sum(item["triggered"] for item in results) == 1
    assert all(item["status"] == "running" for item in results)
    assert len(deferred_threads) == 1
    assert deferred_threads[0].args[0] is analysis._review_job
    assert analysis._review_job.started_at > 0


def test_offline_review_orchestration_and_authenticated_terminal_stream(client, dependencies, deferred_threads):
    response = client.post(PREFIX, headers=client.review_headers)
    assert response.status_code == 200 and response.json()["triggered"]
    job = analysis._review_job
    deferred_threads[0].run()
    result = client.get(PREFIX + "/status", headers=client.review_headers).json()
    assert result["status"] == "done" and result["report"] == dependencies.market.generate_market_review.return_value
    assert result["error"] == "" and len(result["steps"]) == 6
    assert all(step["done"] and step["duration_ms"] >= 0 for step in result["steps"])
    assert job.finished_at >= job.started_at
    dependencies.constructors["stopat30m.analysis.market_analyzer"]["MarketAnalyzer"].assert_called_once_with(
        search_service=dependencies.search, analyzer=dependencies.analyzer)
    dependencies.market.generate_market_review.assert_called_once_with(
        dependencies.market.get_market_overview.return_value, dependencies.market.search_market_news.return_value)
    dependencies.notifier.save_report_to_file.assert_called_once()
    report, filename = dependencies.notifier.save_report_to_file.call_args.args
    assert result["report"] in report and re.fullmatch(r"market_review_\d{8}\.md", filename)
    dependencies.notifier.send.assert_called_once_with(f"🎯 大盘复盘\n\n{result['report']}", email_send_to_all=True)
    terminal = client.get(PREFIX + "/stream", headers=client.review_headers)
    assert event_payload(terminal)[0]["status"] == "done"


def test_worker_keeps_its_original_job_object(client, dependencies, deferred_threads, monkeypatch):
    client.post(PREFIX, headers=client.review_headers)
    original = analysis._review_job
    later = analysis.ReviewJob()
    monkeypatch.setattr(analysis, "_review_job", later)
    deferred_threads[0].run()
    assert original.status == analysis.ReviewStatus.DONE
    assert original.report == dependencies.market.generate_market_review.return_value
    assert later.status == analysis.ReviewStatus.IDLE and later.steps == [] and later.report == ""


@pytest.mark.parametrize("available", [False, True])
def test_notification_skip_or_false_result_is_not_reported_as_sent(client, dependencies, deferred_threads, available):
    dependencies.notifier.is_available.return_value = available
    dependencies.notifier.send.return_value = False
    client.post(PREFIX, headers=client.review_headers)
    deferred_threads[0].run()
    result = client.get(PREFIX + "/status", headers=client.review_headers).json()
    assert result["status"] == "done" and result["report"]
    last = result["steps"][-1]
    if available:
        assert not last["done"] and last["message"] == "通知发送未确认成功，报告已生成"
        dependencies.notifier.send.assert_called_once()
    else:
        assert last["done"] and last["message"] == "未配置通知，已跳过"
        dependencies.notifier.send.assert_not_called()


def test_missing_dependency_has_initialization_step_and_fixed_nonsecret_hint(client, dependencies, deferred_threads, monkeypatch):
    original_import = builtins.__import__
    sentinel = "offline-sensitive-provider-fragment"
    def missing_dependency(name, *args, **kwargs):
        if name == "stopat30m.notification.service":
            raise ModuleNotFoundError(sentinel, name="markdown2")
        return original_import(name, *args, **kwargs)
    client.post(PREFIX, headers=client.review_headers)
    with monkeypatch.context() as context:
        context.setattr(builtins, "__import__", missing_dependency)
        deferred_threads[0].run()
    result = client.get(PREFIX + "/status", headers=client.review_headers).json()
    assert result["status"] == "failed"
    assert result["steps"] == [{"message": "初始化分析组件", "done": False, "duration_ms": result["steps"][0]["duration_ms"]}]
    assert "markdown2" in result["error"] and "requirements-web.txt" in result["error"]
    assert sentinel not in json.dumps(result)
    assert not dependencies.notifier.save_report_to_file.called and not dependencies.notifier.send.called


@pytest.mark.parametrize("error_status", [401, 404, 500])
def test_failed_llm_call_is_not_a_web_authentication_failure_or_secret_echo(client, dependencies, deferred_threads, error_status):
    class UpstreamFailure(RuntimeError):
        status_code = error_status
    sentinel = "offline-sensitive-upstream-response"
    dependencies.market.generate_market_review.side_effect = UpstreamFailure(sentinel)
    client.post(PREFIX, headers=client.review_headers)
    deferred_threads[0].run()
    response = client.get(PREFIX + "/stream", headers=client.review_headers)
    assert response.status_code == 200
    result = event_payload(response)[0]
    assert result["status"] == "failed" and result["steps"][3]["message"] == "大模型生成复盘报告"
    assert result["steps"][2]["done"] and not result["steps"][3]["done"]
    assert sentinel not in response.text
    if error_status == 401:
        assert "HTTP 401" in result["error"] and "不是网页登录失效" in result["error"]
    elif error_status == 404:
        assert "HTTP 404" in result["error"] and "模型列表" in result["error"]
    assert client.get("/api/v1/auth/me", headers=client.review_headers).status_code == 200
    assert not dependencies.notifier.send.called and not dependencies.notifier.save_report_to_file.called


def test_thread_start_failure_is_recorded_without_stuck_running_slot(client, monkeypatch, deferred_threads):
    class UnavailableThread:
        def __init__(self, **kwargs): pass
        def start(self): raise RuntimeError("offline-sensitive-thread-error")
    with monkeypatch.context() as context:
        context.setattr(analysis, "threading", SimpleNamespace(Thread=UnavailableThread))
        response = client.post(PREFIX, headers=client.review_headers)
        assert response.status_code == 503 and "offline-sensitive" not in response.text
    result = client.get(PREFIX + "/status", headers=client.review_headers).json()
    assert result["status"] == "failed" and result["steps"][0]["message"] == "启动复盘后台任务"
    retry = client.post(PREFIX, headers=client.review_headers)
    assert retry.json()["triggered"] and retry.json()["status"] == "running"
