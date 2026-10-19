"""A DB backup must contain committed WAL data, never config credentials."""
import io
import json
from pathlib import Path
import sqlite3
import zipfile

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from types import SimpleNamespace

from stopat30m.api.deps import get_db_session
from stopat30m.api.v1.endpoints import operations
from stopat30m.auth.deps import get_current_user
from stopat30m.operations import BackupError, BackupStore
from stopat30m.research_workspace import ResearchWorkspace
from stopat30m.storage.models import Base, User


def test_backup_includes_committed_wal_and_is_independent_of_later_writes(tmp_path):
    source = tmp_path / "live.db"
    live = sqlite3.connect(source)
    live.execute("PRAGMA journal_mode=WAL")
    live.execute("CREATE TABLE ledger(amount INTEGER)")
    live.execute("INSERT INTO ledger VALUES (1234)")
    live.commit()
    (tmp_path / "config.local.yaml").write_text("llm:\n  openai_api_key: never-export-test-secret\n")
    store = BackupStore(root=tmp_path / "backups")
    result = store.create(source)
    archive = store.archive(result["id"])
    live.execute("INSERT INTO ledger VALUES (9999)")
    live.commit()
    live.close()
    with zipfile.ZipFile(archive) as bundle:
        assert sorted(bundle.namelist()) == ["account.sqlite3", "manifest.json"]
        assert b"never-export-test-secret" not in bundle.read("manifest.json")
        restored = tmp_path / "verify.sqlite3"
        restored.write_bytes(bundle.read("account.sqlite3"))
    with sqlite3.connect(restored) as db:
        assert db.execute("SELECT amount FROM ledger").fetchall() == [(1234,)]
        assert db.execute("PRAGMA quick_check").fetchone() == ("ok",)
    assert store.list()[0]["validation_status"] == "valid"
    assert archive.stat().st_mode & 0o777 == 0o600


def test_bad_backups_do_not_hide_good_ones_and_paths_cannot_escape(tmp_path):
    source = tmp_path / "live.db"
    with sqlite3.connect(source) as db:
        db.execute("CREATE TABLE ledger(id INTEGER)")
    store = BackupStore(root=tmp_path / "backups")
    good, bad = store.create(source), store.create(source)
    (store.root / bad["id"] / "archive.zip").write_bytes(b"corrupt")
    rows = {row["id"]: row for row in store.list()}
    assert rows[good["id"]]["validation_status"] == "valid"
    assert rows[bad["id"]]["validation_status"] == "invalid"
    with pytest.raises(BackupError):
        store.archive("../live.db")
    with pytest.raises(BackupError):
        store.archive(bad["id"])
    outside = tmp_path / "outside"
    outside.mkdir()
    alias = store.root / "backup_20200101_000000_12345678"
    alias.symlink_to(outside, target_is_directory=True)
    with pytest.raises(BackupError, match="symlink"):
        store.archive(alias.name)


@pytest.mark.parametrize("missing_report_component", [None, "markdown2", "litellm", "json_repair", "akshare", "efinance"])
def test_admin_can_check_create_and_download_backup_without_market_network(tmp_path, monkeypatch, missing_report_component):
    monkeypatch.setattr(operations, "find_spec", lambda module: None if module == missing_report_component else object())
    engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    store = BackupStore(root=tmp_path / "backups")
    app = FastAPI()
    app.include_router(operations.router, prefix="/api/v1")
    def sessions():
        with factory.begin() as db:
            yield db
    app.dependency_overrides[get_db_session] = sessions
    app.dependency_overrides[operations.get_backup_store] = lambda: store
    app.dependency_overrides[operations.get_workspace] = lambda: ResearchWorkspace(root=tmp_path / "research")
    with TestClient(app) as client:
        assert client.get("/api/v1/system/backups").status_code == 401
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=1, role="user")
        assert client.post("/api/v1/system/backups").status_code == 403
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=1, role="admin")
        status = client.get("/api/v1/system/operations")
        assert status.status_code == 200 and status.json()["external_services_checked"] is False
        report_check = next(item for item in status.json()["checks"] if item["id"] == "reports")
        assert report_check["status"] == ("pending" if missing_report_component else "ok")
        if missing_report_component:
            assert missing_report_component in report_check["message"]
            assert "requirements-web.txt" in report_check["message"]
        assert client.get("/api/v1/system/backups").json() == []
        result = client.post("/api/v1/system/backups")
        assert result.status_code == 201, result.text
        identifier = result.json()["id"]
        download = client.get(f"/api/v1/system/backups/{identifier}/download")
        assert download.status_code == 200 and download.headers["cache-control"] == "no-store"
        with zipfile.ZipFile(io.BytesIO(download.content)) as bundle:
            assert json.loads(bundle.read("manifest.json"))["id"] == identifier
    engine.dispose()
