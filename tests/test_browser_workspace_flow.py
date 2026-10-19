"""Actual app: browser setup, settings, cash NAV, reconciliation and backup."""
import io
from pathlib import Path
import sqlite3
import zipfile

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopat30m import config
from stopat30m.api.app import create_app
from stopat30m.api.v1.endpoints import operations, research, settings
from stopat30m.auth import service
from stopat30m.data import realtime
from stopat30m.operations import BackupStore
from stopat30m.research_workspace import ResearchWorkspace
from stopat30m.settings_store import SettingsStore
from stopat30m.storage import database
from stopat30m.trading.portfolio import today


def test_first_use_and_daily_account_workflow_without_commands_or_external_services(tmp_path, monkeypatch):
    base = tmp_path / "config.yaml"
    public = (config.PROJECT_ROOT / "config.yaml").read_bytes()
    base.write_bytes(public)
    local = tmp_path / "config.local.yaml"
    local.write_text('llm:\n  openai_api_key: "isolated-seed-key"\n')
    monkeypatch.setattr(config, "_DEFAULT_CONFIG_PATH", base)
    monkeypatch.setattr(config, "_config_cache", None)
    monkeypatch.setattr(service, "_secret_key", None)
    monkeypatch.delenv("STOPAT30M_AUTH__SECRET_KEY", raising=False)
    engine = create_engine(f"sqlite:///{tmp_path / 'account.db'}", connect_args={"check_same_thread": False})
    monkeypatch.setattr(database, "_engine", engine)
    monkeypatch.setattr(database, "_SessionFactory", sessionmaker(bind=engine, expire_on_commit=False))
    def no_network(*args, **kwargs):
        raise AssertionError("This cash-only workflow must not request market prices")
    monkeypatch.setattr(realtime, "fetch_spot_quotes", no_network)
    app = create_app(static_dir=str(tmp_path / "no-build"))
    app.dependency_overrides[settings.get_settings_store] = lambda: SettingsStore(base, environ={})
    app.dependency_overrides[research.get_workspace] = lambda: ResearchWorkspace(root=tmp_path / "research")
    store = BackupStore(root=tmp_path / "backups")
    app.dependency_overrides[operations.get_backup_store] = lambda: store
    try:
        with TestClient(app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 50000)) as client:
            assert client.get("/api/v1/auth/bootstrap").json()["allowed"] is True
            setup = client.post("/api/v1/auth/bootstrap", json={"username": "browser-owner", "password": "offline-browser-password"})
            assert setup.status_code == 201, setup.text
            headers = {"Authorization": f"Bearer {setup.json()['access_token']}"}
            current = client.get("/api/v1/system/settings", headers=headers)
            assert current.status_code == 200, current.text
            assert "isolated-seed-key" not in current.text
            saved = client.put("/api/v1/system/settings", headers=headers, json={
                "revision": current.json()["revision"], "values": {"research.min_interval": 4}, "clear_secrets": [],
            })
            assert saved.status_code == 200, saved.text
            assert base.read_bytes() == public
            assert "isolated-seed-key" in local.read_text()
            assert "isolated-seed-key" not in saved.text
            invalid = client.post("/api/v1/auth/change-password", headers=headers, json={
                "old_password": "offline-browser-password", "new_password": "pvt",
            })
            assert invalid.status_code == 422
            assert "pvt" not in invalid.text and "offline-browser-password" not in invalid.text

            day = today().isoformat()
            initialized = client.post("/api/v1/trading/account/initialize", headers=headers, json={
                "initial_capital": 10000, "opened_date": day, "idempotency_key": "init-browser",
            })
            assert initialized.status_code == 200, initialized.text
            deposit = client.post("/api/v1/trading/cash-flows", headers=headers, json={
                "direction": "DEPOSIT", "amount": 500, "effective_date": day, "idempotency_key": "deposit-browser",
            })
            assert deposit.status_code == 200, deposit.text
            nav = client.post("/api/v1/trading/nav", headers=headers, json={"idempotency_key": "nav-browser"})
            assert nav.status_code == 200, nav.text
            assert nav.json()["snapshot"]["equity"] == 10500
            assert nav.json()["snapshot"]["total_pnl"] == 0
            replay = client.post("/api/v1/trading/nav", headers=headers, json={"idempotency_key": "nav-browser"})
            assert replay.json()["replayed"] is True
            statement = client.post("/api/v1/trading/reconcile", headers=headers, json={
                "csv_text": "instrument,quantity\n600000,100\n", "broker_cash": 10500, "statement_date": day,
            })
            assert statement.status_code == 200, statement.text
            assert statement.json()["matched"] is False
            assert statement.json()["positions"][0]["difference"] == 100
            # A reconciliation result is read-only and must never create a fill.
            assert client.get("/api/v1/trading/trades", headers=headers).json()["total"] == 0

            backup = client.post("/api/v1/system/backups", headers=headers)
            assert backup.status_code == 201, backup.text
            bundle = client.get(f"/api/v1/system/backups/{backup.json()['id']}/download", headers=headers)
            assert bundle.status_code == 200
            with zipfile.ZipFile(io.BytesIO(bundle.content)) as archive:
                copied = tmp_path / "copied.db"
                copied.write_bytes(archive.read("account.sqlite3"))
            with sqlite3.connect(copied) as db:
                assert db.execute("SELECT COUNT(*) FROM portfolio_nav").fetchone() == (1,)
                assert db.execute("SELECT COUNT(*) FROM portfolio_nav_receipts").fetchone() == (1,)
            status = client.get("/api/v1/system/operations", headers=headers)
            assert status.status_code == 200, status.text
            checks = {item["id"]: item["status"] for item in status.json()["checks"]}
            assert checks["account"] == checks["backup"] == "ok"
            assert checks["research"] == "pending"
    finally:
        engine.dispose()
