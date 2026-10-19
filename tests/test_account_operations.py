"""Personal observed NAV and statement matching: all quotes are synthetic."""
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from threading import Barrier
from unittest.mock import Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from stopat30m.api.deps import get_db_session
from stopat30m.api.v1.endpoints.account_operations import router
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import Base, PortfolioNav, PortfolioNavReceipt, User
from stopat30m.trading import account_operations as operations, portfolio

DAY = date(2026, 10, 19)
NOW = datetime(2026, 10, 19, 10, 0, 30, tzinfo=portfolio.SHANGHAI)


def quote(price=11):
    return {"SH600000": {"price": price, "source": "offline_fixture", "as_of": (NOW - timedelta(seconds=10)).isoformat(),
        "fetched_at": NOW.isoformat(), "status": "available", "is_realtime": True}}


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'account.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    with sessions.begin() as db:
        db.add_all([User(id=1, username="owner", password_hash="unused"), User(id=2, username="other", password_hash="unused")])
    with patch.object(portfolio, "today", return_value=DAY), patch.object(operations, "now", return_value=NOW):
        yield engine, sessions
    engine.dispose()


def initialize(sessions, owner=1, capital=10000):
    with sessions.begin() as db:
        portfolio.initialize_account(db, owner, capital, "init", opened_date="2026-10-15")


def buy(sessions, key="buy", quantity=100):
    with sessions.begin() as db:
        portfolio.record_trade(db, 1, "600000", "BUY", quantity, 10, key,
            trade_date="2026-10-16", commission=1, stamp_tax=0, transfer_fee=0)


def save(sessions, key="nav", loader=None, user=1):
    with sessions() as db:
        result = operations.save_nav(db, user, key, quote_loader=loader)
        db.commit()
        return result


def test_cash_only_snapshot_and_external_cash_is_not_profit(store):
    _, sessions = store
    initialize(sessions)
    loader = Mock(side_effect=AssertionError("cash account must not query prices"))
    first, replay = save(sessions, loader=loader)
    assert not replay and first["equity"] == 10000 and first["total_pnl"] == 0
    with sessions.begin() as db:
        portfolio.record_cash_flow(db, 1, "DEPOSIT", 2000, "deposit", effective_date=DAY)
    second, _ = save(sessions, "after_deposit", loader)
    assert second["id"] == first["id"] and second["revision"] == 2
    assert second["equity"] == second["net_deposits"] == 12000
    assert second["total_pnl"] == 0 and second["ledger_version"] != first["ledger_version"]
    with sessions.begin() as db:
        portfolio.record_cash_flow(db, 1, "WITHDRAW", 500, "withdraw", effective_date=DAY)
    third, _ = save(sessions, "after_withdraw", loader)
    assert third["equity"] == 11500 and third["total_pnl"] == 0
    assert not loader.called


def test_snapshot_preserves_prices_fees_and_idempotent_receipts_after_same_day_update(store):
    _, sessions = store
    initialize(sessions); buy(sessions)
    first, replay = save(sessions, loader=lambda _: quote())
    assert not replay and first["cash"] == 8999 and first["market_value"] == 1100
    assert first["equity"] == 10099 and first["total_pnl"] == first["unrealized_pnl"] == 99
    assert first["total_fees"] == 1
    assert first["facts"]["quotes"]["SH600000"] == quote()["SH600000"] | {"name": ""}
    second, _ = save(sessions, "new_quote", lambda _: quote(12))
    assert second["id"] == first["id"] and second["revision"] == 2 and second["equity"] == 10199
    forbidden_loader = Mock(side_effect=AssertionError("a retry must not fetch quotes"))
    replayed, replay = save(sessions, loader=forbidden_loader)
    assert replay and replayed == first
    assert not forbidden_loader.called
    with sessions() as db:
        assert db.query(PortfolioNav).count() == 1 and db.query(PortfolioNavReceipt).count() == 2
        assert operations.list_nav(db, 1)["items"][0] == second


def test_saved_realized_and_unrealized_pnl_reconcile_to_equity_less_external_cash(store):
    _, sessions = store
    initialize(sessions); buy(sessions)
    with sessions.begin() as db:
        portfolio.record_trade(db, 1, "600000", "SELL", 40, 12, "sell", trade_date=DAY,
            commission=1, stamp_tax=0.24, transfer_fee=0)
    snapshot, _ = save(sessions, loader=lambda _: quote())
    assert snapshot["realized_pnl"] == 78.36 and snapshot["unrealized_pnl"] == 59.4
    assert snapshot["total_pnl"] == 137.76 and snapshot["equity"] == 10137.76
    assert snapshot["total_fees"] == 2.24


def test_simultaneous_same_key_creates_one_daily_snapshot_and_one_receipt(store):
    _, sessions = store
    initialize(sessions); buy(sessions)
    both_read = Barrier(2)
    def price_loader(_):
        both_read.wait(timeout=5)
        return quote()
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(save, sessions, "same", price_loader) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert sorted(result[1] for result in results) == [False, True]
    assert results[0][0] == results[1][0]
    with sessions() as db:
        assert db.query(PortfolioNav).one().revision == 1
        assert db.query(PortfolioNavReceipt).count() == 1


@pytest.mark.parametrize("fault", ["missing", "stale", "unknown_source", "missing_time", "naive_time", "old_time", "future_time", "old_fetch", "not_realtime", "nan_price"])
def test_incomplete_or_untrusted_quote_never_creates_a_snapshot(store, fault):
    _, sessions = store
    initialize(sessions); buy(sessions)
    quotes = quote()
    if fault == "missing": quotes = {}
    elif fault == "stale": quotes["SH600000"]["status"] = "stale"
    elif fault == "unknown_source": quotes["SH600000"]["source"] = None
    elif fault == "missing_time": quotes["SH600000"]["as_of"] = None
    elif fault == "naive_time": quotes["SH600000"]["as_of"] = NOW.replace(tzinfo=None).isoformat()
    elif fault == "old_time": quotes["SH600000"]["as_of"] = (NOW - timedelta(minutes=3)).isoformat()
    elif fault == "future_time": quotes["SH600000"]["as_of"] = (NOW + timedelta(seconds=1)).isoformat()
    elif fault == "old_fetch": quotes["SH600000"]["fetched_at"] = (NOW - timedelta(minutes=3)).isoformat()
    elif fault == "not_realtime": quotes["SH600000"]["is_realtime"] = False
    else: quotes["SH600000"]["price"] = float("nan")
    with pytest.raises(portfolio.PortfolioError): save(sessions, loader=lambda _: quotes)
    with sessions() as db:
        assert db.query(PortfolioNav).count() == db.query(PortfolioNavReceipt).count() == 0


def test_ledger_change_while_fetching_prices_is_rejected_without_holding_writer_lock(store):
    _, sessions = store
    initialize(sessions); buy(sessions)
    def mutate(_):
        with sessions.begin() as other:
            portfolio.record_cash_flow(other, 1, "DEPOSIT", 100, "during_fetch", effective_date=DAY)
        return quote()
    with pytest.raises(portfolio.PortfolioError, match="账本已变化"):
        save(sessions, loader=mutate)
    with sessions() as db:
        assert db.query(PortfolioNav).count() == 0
        assert portfolio.get_account(db, 1).cash == 9099


def test_date_boundary_refuses_new_snapshot_but_next_day_retry_returns_original_receipt(store):
    _, sessions = store
    initialize(sessions)
    first, _ = save(sessions)
    later = NOW + timedelta(days=1)
    with patch.object(operations, "now", return_value=later), patch.object(portfolio, "today", return_value=later.date()):
        original, replayed = save(sessions)
        assert replayed and original == first
        second, _ = save(sessions, "next_day")
        assert second["date"] == "2026-10-20" and second["id"] != first["id"]
    with sessions() as db:
        assert [item["date"] for item in operations.list_nav(db, 1)["items"]] == ["2026-10-20", "2026-10-19"]


def test_crossing_shanghai_midnight_during_save_does_not_create_any_observation(store):
    _, sessions = store
    initialize(sessions)
    with patch.object(operations, "now", side_effect=[NOW, NOW + timedelta(days=1)]), pytest.raises(portfolio.PortfolioError, match="上海日期"):
        save(sessions)
    with sessions() as db:
        assert db.query(PortfolioNav).count() == db.query(PortfolioNavReceipt).count() == 0


def test_owner_isolation_and_uninitialized_operations(store):
    _, sessions = store
    initialize(sessions); save(sessions)
    with sessions() as db:
        assert operations.list_nav(db, 2)["total"] == 0
        with pytest.raises(portfolio.PortfolioError, match="初始化"):
            operations.reconcile(db, 2, "instrument,quantity\n", 0)
    with pytest.raises(portfolio.PortfolioError, match="初始化"):
        save(sessions, user=2)
    initialize(sessions, 2, 777)
    second, replayed = save(sessions, user=2)
    assert not replayed and second["equity"] == 777


def test_reconciliation_is_read_only_and_does_not_fetch_or_validate_prices(store):
    engine, sessions = store
    initialize(sessions); buy(sessions)
    statements = []
    def record_sql(_, __, statement, ___, ____, _____): statements.append(statement)
    event.listen(engine, "before_cursor_execute", record_sql)
    try:
        with patch("stopat30m.data.realtime.fetch_spot_quotes", side_effect=AssertionError("no quote IO")), sessions() as db:
            report = operations.reconcile(db, 1, "\ufeffinstrument,quantity\n600000.SH,100\n", 8999, "2026-10-19")
            assert report["matched"] and report["cash"]["difference"] == 0
            assert report["positions"] == [{"instrument": "SH600000", "ledger_quantity": 100, "statement_quantity": 100, "difference": 0}]
            mismatch = operations.reconcile(db, 1, "instrument,quantity\n600000,90\nSZ000001,10", 9000)
            assert not mismatch["matched"] and mismatch["cash"]["difference"] == 1
            assert [item["difference"] for item in mismatch["positions"]] == [-10, 10]
    finally: event.remove(engine, "before_cursor_execute", record_sql)
    assert not any(re.match(r"\s*(INSERT|UPDATE|DELETE|REPLACE)\b", sql, re.I) for sql in statements)


@pytest.mark.parametrize("csv_text", ["instrument,quantity\n600000,100\nsh.600000,1", "instrument,quantity\n600000,-1",
    "instrument,quantity\n600000,1.5", "instrument,quantity\n600000,NaN", "instrument,quantity\n600000,1e2",
    "instrument,quantity\n600000,1000000001", "instrument,quantity\n600000,10,extra", "code,quantity\n600000,1",
    "instrument,quantity\nINVALID,10", "instrument,quantity\n600000,\"1", "x" * 100001])
def test_csv_validation_rejects_duplicate_normalized_codes_and_invalid_quantities(store, csv_text):
    _, sessions = store
    initialize(sessions)
    with sessions() as db, pytest.raises(portfolio.PortfolioError):
        operations.reconcile(db, 1, csv_text, 10000)


def test_statement_rejects_too_many_rows_and_ill_formed_unicode(store):
    _, sessions = store
    initialize(sessions)
    excessive = "instrument,quantity\n" + "\n".join(f"{100000 + index},0" for index in range(2001))
    for content in (excessive, "instrument,quantity\n\ud800,1"):
        with sessions() as db, pytest.raises(portfolio.PortfolioError):
            operations.reconcile(db, 1, content, 10000)


@pytest.mark.parametrize("day,cash", [("2026-10-18", 0), ("2026-10-20", 0), ("2026-1-1", 0), (None, -1), (None, float("inf")), (None, 9999.999)])
def test_statement_date_and_cash_are_strict(store, day, cash):
    _, sessions = store
    initialize(sessions)
    with sessions() as db, pytest.raises(portfolio.PortfolioError):
        operations.reconcile(db, 1, "instrument,quantity\n", cash, day)


def test_http_ownership_commit_and_reconcile_contract(store):
    _, sessions = store
    initialize(sessions)
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    owner = [1]
    def session_dependency():
        with sessions() as db:
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise
    app.dependency_overrides[get_db_session] = session_dependency
    app.dependency_overrides[get_current_user] = lambda: User(id=owner[0], username="test")
    with TestClient(app) as client, patch("stopat30m.data.realtime.fetch_spot_quotes", side_effect=AssertionError("GET/cash-only must not fetch")):
        response = client.post("/api/v1/trading/nav", json={"idempotency_key": "cash"})
        assert response.status_code == 200 and not response.json()["replayed"]
        assert client.get("/api/v1/trading/nav?limit=1").json()["total"] == 1
        assert client.post("/api/v1/trading/reconcile", json={"csv_text": "instrument,quantity\n", "broker_cash": 10000}).json()["matched"]
        assert client.post("/api/v1/trading/reconcile", json={"csv_text": "instrument,quantity\n", "broker_cash": True}).status_code == 422
        assert client.post("/api/v1/trading/nav", json={"idempotency_key": "bad", "date": "2020-01-01"}).status_code == 422
        owner[0] = 2
        assert client.get("/api/v1/trading/nav").json()["items"] == []
        assert client.post("/api/v1/trading/nav", json={"idempotency_key": "other"}).status_code == 409
        owner[0] = 1
        with patch.object(Session, "commit", side_effect=RuntimeError("offline disk failure")), pytest.raises(RuntimeError):
            client.post("/api/v1/trading/nav", json={"idempotency_key": "disk_fail"})
    with sessions() as db:
        assert db.query(PortfolioNav).one().revision == 1
        assert db.query(PortfolioNavReceipt).count() == 1
