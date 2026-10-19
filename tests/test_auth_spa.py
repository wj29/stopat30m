from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from stopat30m.api.spa import SPAStaticFiles
from stopat30m.api.v1.endpoints import auth
from stopat30m.auth import deps


def test_personal_mode_disables_registration(monkeypatch):
    monkeypatch.setattr(auth, "get", lambda section, key, default=None: {"single_user": True, "registration_enabled": True}.get(key, default))
    app = FastAPI()
    app.include_router(auth.router)
    app.dependency_overrides[auth.get_db_session] = lambda: None
    with TestClient(app) as client:
        assert client.get("/auth/settings").json() == {"single_user": True, "registration_enabled": False}
        response = client.post("/auth/register", json={"username": "owner", "password": "password123", "invite_code": "unused"})
        assert response.status_code == 403


def test_invalid_signed_subject_returns_401(monkeypatch):
    monkeypatch.setattr(deps, "decode_access_token", lambda token: {"sub": "not-an-id"})
    with pytest.raises(HTTPException) as error:
        deps.get_current_user(bearer_token="dummy", query_token=None, db=SimpleNamespace())
    assert error.value.status_code == 401


def test_spa_routes_fallback_without_masking_missing_api_or_assets(tmp_path):
    (tmp_path / "index.html").write_text("<main>Personal trading workspace</main>")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets/app.js").write_text("export const ready = true;")
    app = FastAPI()
    app.mount("/", SPAStaticFiles(directory=tmp_path, html=True))
    with TestClient(app) as client:
        assert "Personal trading" in client.get("/trading").text
        assert "Personal trading" in client.get("/model-settings").text
        assert client.head("/backtest").status_code == 200
        assert client.head("/backtest").content == b""
        assert client.get("/assets/app.js").status_code == 200
        for path in ("/api/v1/missing", "/assets/missing.js", "/unknown"):
            response = client.get(path)
            assert response.status_code == 404
            assert "Personal trading" not in response.text
        assert client.post("/trading").status_code == 405
