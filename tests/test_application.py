"""Exercise the actual application factory, auth, database and account together."""

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopat30m.api.app import create_app
from stopat30m.api.v1.endpoints import system
from stopat30m.auth.service import hash_password
from stopat30m.data import provider, realtime
from stopat30m.storage import database
from stopat30m.storage.models import User


def test_personal_workspace_end_to_end_without_market_network(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'integration.db'}", connect_args={"check_same_thread": False})
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(database, "_engine", engine)
    monkeypatch.setattr(database, "_SessionFactory", factory)
    monkeypatch.setattr(provider, "get_data_dir", lambda: tmp_path / "market")
    monkeypatch.setattr(system, "get_data_dir", lambda: tmp_path / "market")
    monkeypatch.setattr(realtime, "fetch_spot_quotes", lambda instruments: {})
    with TestClient(create_app(static_dir=str(tmp_path / "no-static-files"))) as client:
        assert client.get("/api/v1/health").status_code == 200
        assert client.get("/api/v1/auth/settings").json()["single_user"] is True
        assert client.get("/api/v1/trading/account").status_code == 401
        with factory.begin() as db:
            db.add(User(username="test-owner", password_hash=hash_password("offline-password"), role="admin"))
        login = client.post("/api/v1/auth/login", json={"username": "test-owner", "password": "offline-password"})
        assert login.status_code == 200
        headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
        assert client.get("/api/v1/trading/account", headers=headers).json()["initialized"] is False
        day = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        initialized = client.post("/api/v1/trading/account/initialize", headers=headers, json={
            "initial_capital": 10000, "opened_date": day, "idempotency_key": "init-integration",
        })
        assert initialized.status_code == 200, initialized.text
        assert initialized.json()["account"]["cash"] == 10000
        body = {"instrument": "600000", "direction": "BUY", "quantity": 100, "price": 10,
                "trade_date": day, "idempotency_key": "buy-integration"}
        bought = client.post("/api/v1/trading/trade", headers=headers, json=body)
        assert bought.status_code == 200, bought.text
        replay = client.post("/api/v1/trading/trade", headers=headers, json=body)
        assert replay.json()["id"] == bought.json()["id"]
        assert replay.json()["replayed"] is True
        positions = client.get("/api/v1/trading/positions", headers=headers).json()
        assert positions["positions"][0]["quantity"] == 100
        assert positions["positions"][0]["available_quantity"] == 0
        assert positions["positions"][0]["current_price"] is None
        assert positions["valuation_complete"] is False
        sold = client.post("/api/v1/trading/trade", headers=headers, json={
            **body, "direction": "SELL", "idempotency_key": "sell-integration",
        })
        assert sold.status_code == 409
        history = client.get("/api/v1/trading/trades?limit=1", headers=headers).json()
        assert history["total"] == 1
        assert len(history["items"]) == 1
        assert client.get("/api/v1/system/data-status", headers=headers).json()["status"] == "missing"
        assert client.get("/api/v1/backtest/defaults", headers=headers).json()["supported_methods"] == ["top_k"]
    engine.dispose()
